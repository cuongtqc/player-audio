const express = require('express');
const path = require('path');
const fs = require('fs');
const { pipeline } = require('stream/promises');
const { spawn } = require('child_process');
const ytdl = require('ytdl-core');

const app = express();
const PORT = process.env.PORT || 3000;
const DOWNLOAD_DIR = path.join(__dirname, 'downloads');

app.use(express.static(path.join(__dirname, 'public')));

const RATE_WINDOW_MS = 60_000;
const RATE_MAX = 30;
const rateState = new Map();

app.use((req, res, next) => {
  const ip = req.ip || req.connection.remoteAddress || 'unknown';
  const now = Date.now();
  const entry = rateState.get(ip) || { count: 0, resetAt: now + RATE_WINDOW_MS };
  if (now > entry.resetAt) {
    entry.count = 0;
    entry.resetAt = now + RATE_WINDOW_MS;
  }
  entry.count += 1;
  rateState.set(ip, entry);
  if (entry.count > RATE_MAX) {
    res.status(429).json({ error: 'Too many requests. Please slow down.' });
    return;
  }
  next();
});

function sanitizeFilename(name) {
  return name.replace(/[<>:"/\\|?*]+/g, '').replace(/\s+/g, ' ').trim();
}

function parseQuality(value) {
  if (!value) return 'highest';
  if (value === 'highest' || value === 'lowest') return value;
  const itag = Number(value);
  if (Number.isFinite(itag)) return itag;
  return 'highest';
}

function isValidYoutubeUrl(value) {
  try {
    const url = new URL(value);
    const host = url.hostname.replace(/^www\./, '');
    return (
      host === 'youtube.com' ||
      host === 'youtu.be' ||
      host.endsWith('.youtube.com')
    );
  } catch (err) {
    return false;
  }
}

function pickFormat(info, { type, quality, enableFfmpeg }) {
  const qualityPref = parseQuality(quality);
  if (type === 'audio') {
    const audioFormats = ytdl.filterFormats(info.formats, 'audioonly');
    const format = ytdl.chooseFormat(audioFormats, { quality: qualityPref });
    return { format, isMuxed: false };
  }

  const muxed = info.formats.filter((fmt) => fmt.hasVideo && fmt.hasAudio);
  if (muxed.length) {
    const format = ytdl.chooseFormat(muxed, { quality: qualityPref });
    return { format, isMuxed: true };
  }

  if (!enableFfmpeg) {
    return { format: null, isMuxed: false };
  }

  const videoOnly = info.formats.filter((fmt) => fmt.hasVideo && !fmt.hasAudio);
  const audioOnly = info.formats.filter((fmt) => fmt.hasAudio && !fmt.hasVideo);
  const videoFormat = ytdl.chooseFormat(videoOnly, { quality: qualityPref });
  const audioFormat = ytdl.chooseFormat(audioOnly, { quality: 'highest' });
  return { format: null, audioFormat, videoFormat, isMuxed: false };
}

function parseRangeHeader(rangeHeader, size) {
  if (!rangeHeader || !size) return null;
  const match = /bytes=(\d+)-(\d*)/i.exec(rangeHeader);
  if (!match) return null;
  const start = Number(match[1]);
  const end = match[2] ? Number(match[2]) : size - 1;
  if (!Number.isFinite(start) || !Number.isFinite(end) || start > end) return null;
  return { start, end };
}

function mapYtdlError(err) {
  const message = String(err && err.message ? err.message : err);
  if (message.includes('private')) return { status: 403, message: 'Video is private.' };
  if (message.includes('age-restricted')) return { status: 403, message: 'Video is age restricted.' };
  if (message.includes('410')) return { status: 410, message: 'Video is no longer available.' };
  if (message.includes('403')) return { status: 403, message: 'Access forbidden (possible throttling).' };
  if (message.includes('Live')) return { status: 400, message: 'Livestreams are not supported.' };
  return { status: 500, message: 'Failed to fetch media.' };
}

app.get('/api/media', async (req, res) => {
  const {
    url,
    mode = 'stream',
    type = 'video',
    quality = 'highest',
    downloadTarget = 'response',
    filename,
    enableFfmpeg = 'false',
  } = req.query;

  if (!url || typeof url !== 'string' || !isValidYoutubeUrl(url) || !ytdl.validateURL(url)) {
    res.status(400).json({ error: 'Invalid YouTube URL.' });
    return;
  }

  const isDownload = mode === 'download';
  const useFfmpeg = enableFfmpeg === 'true' || enableFfmpeg === '1';

  let info;
  try {
    info = await ytdl.getInfo(url);
  } catch (err) {
    const mapped = mapYtdlError(err);
    res.status(mapped.status).json({ error: mapped.message });
    return;
  }

  const title = sanitizeFilename(info.videoDetails.title || 'youtube-media');
  const defaultName = filename ? sanitizeFilename(filename) : title;

  const formatSelection = pickFormat(info, { type, quality, enableFfmpeg: useFfmpeg });
  if (!formatSelection.format && !formatSelection.videoFormat) {
    res.status(409).json({
      error: 'No muxed format available. Enable ffmpeg to merge video+audio streams.',
    });
    return;
  }

  const range = parseRangeHeader(req.headers.range, formatSelection.format?.contentLength);

  const responseFilename = (() => {
    const ext = formatSelection.format?.container || (type === 'audio' ? 'm4a' : 'mp4');
    if (defaultName.endsWith(`.${ext}`)) return defaultName;
    return `${defaultName}.${ext}`;
  })();

  const isDisk = isDownload && downloadTarget === 'disk';
  const contentDisposition = `${isDownload ? 'attachment' : 'inline'}; filename="${responseFilename}"`;

  let activeStreams = [];
  let ffmpegProcess = null;

  const cleanup = () => {
    activeStreams.forEach((stream) => stream.destroy());
    activeStreams = [];
    if (ffmpegProcess) {
      ffmpegProcess.kill('SIGKILL');
      ffmpegProcess = null;
    }
  };

  req.on('close', cleanup);
  res.on('close', cleanup);

  try {
    if (type === 'audio') {
      const format = formatSelection.format;
      const mime = format.mimeType ? format.mimeType.split(';')[0] : 'audio/mp4';
      const wantMp3 = useFfmpeg && responseFilename.endsWith('.mp3');

      if (!isDisk) {
        res.setHeader('Content-Disposition', contentDisposition);
        res.setHeader('Cache-Control', 'no-store');
      }

      if (!isDisk) {
        res.setHeader('Content-Type', wantMp3 ? 'audio/mpeg' : mime);
      }

      const audioStream = ytdl.downloadFromInfo(info, { format });
      activeStreams.push(audioStream);

      if (wantMp3) {
        ffmpegProcess = spawn('ffmpeg', ['-loglevel', 'error', '-i', 'pipe:3', '-vn', '-c:a', 'libmp3lame', '-b:a', '192k', '-f', 'mp3', 'pipe:1'], {
          stdio: ['ignore', 'pipe', 'pipe', 'pipe'],
        });
        audioStream.pipe(ffmpegProcess.stdio[3]);
        ffmpegProcess.stderr.on('data', (data) => console.warn(data.toString()));
        const outputStream = ffmpegProcess.stdout;
        if (isDisk) {
          await fs.promises.mkdir(DOWNLOAD_DIR, { recursive: true });
          const destPath = path.join(DOWNLOAD_DIR, responseFilename);
          await pipeline(outputStream, fs.createWriteStream(destPath));
          const stat = await fs.promises.stat(destPath);
          res.json({ ok: true, filename: responseFilename, path: destPath, sizeBytes: stat.size });
          return;
        }
        await pipeline(outputStream, res);
        return;
      }

      if (isDisk) {
        await fs.promises.mkdir(DOWNLOAD_DIR, { recursive: true });
        const destPath = path.join(DOWNLOAD_DIR, responseFilename);
        await pipeline(audioStream, fs.createWriteStream(destPath));
        const stat = await fs.promises.stat(destPath);
        res.json({ ok: true, filename: responseFilename, path: destPath, sizeBytes: stat.size });
        return;
      }

      await pipeline(audioStream, res);
      return;
    }

    if (formatSelection.format) {
      const format = formatSelection.format;
      const mime = format.mimeType ? format.mimeType.split(';')[0] : 'video/mp4';

      if (!isDisk) {
        res.setHeader('Content-Disposition', contentDisposition);
        res.setHeader('Cache-Control', 'no-store');
        res.setHeader('Content-Type', mime);
        res.setHeader('Accept-Ranges', 'bytes');
      }

      const ytdlOptions = { format };
      if (range) {
        ytdlOptions.range = range;
        if (!isDisk) {
          res.status(206);
          res.setHeader('Content-Range', `bytes ${range.start}-${range.end}/${format.contentLength}`);
          res.setHeader('Content-Length', range.end - range.start + 1);
        }
      } else if (!isDisk && format.contentLength) {
        res.setHeader('Content-Length', format.contentLength);
      }

      const mediaStream = ytdl.downloadFromInfo(info, ytdlOptions);
      activeStreams.push(mediaStream);

      if (isDisk) {
        await fs.promises.mkdir(DOWNLOAD_DIR, { recursive: true });
        const destPath = path.join(DOWNLOAD_DIR, responseFilename);
        await pipeline(mediaStream, fs.createWriteStream(destPath));
        const stat = await fs.promises.stat(destPath);
        res.json({ ok: true, filename: responseFilename, path: destPath, sizeBytes: stat.size });
        return;
      }

      await pipeline(mediaStream, res);
      return;
    }

    if (useFfmpeg && formatSelection.videoFormat && formatSelection.audioFormat) {
      const videoStream = ytdl.downloadFromInfo(info, { format: formatSelection.videoFormat });
      const audioStream = ytdl.downloadFromInfo(info, { format: formatSelection.audioFormat });
      activeStreams.push(videoStream, audioStream);

      ffmpegProcess = spawn('ffmpeg', ['-loglevel', 'error', '-i', 'pipe:3', '-i', 'pipe:4', '-map', '0:v:0', '-map', '1:a:0', '-c', 'copy', '-f', 'mp4', 'pipe:1'], {
        stdio: ['ignore', 'pipe', 'pipe', 'pipe', 'pipe'],
      });

      videoStream.pipe(ffmpegProcess.stdio[3]);
      audioStream.pipe(ffmpegProcess.stdio[4]);

      ffmpegProcess.stderr.on('data', (data) => console.warn(data.toString()));

      if (!isDisk) {
        res.setHeader('Content-Disposition', contentDisposition);
        res.setHeader('Cache-Control', 'no-store');
        res.setHeader('Content-Type', 'video/mp4');
      }

      if (isDisk) {
        await fs.promises.mkdir(DOWNLOAD_DIR, { recursive: true });
        const destPath = path.join(DOWNLOAD_DIR, responseFilename.replace(/\.[^/.]+$/, '.mp4'));
        await pipeline(ffmpegProcess.stdout, fs.createWriteStream(destPath));
        const stat = await fs.promises.stat(destPath);
        res.json({ ok: true, filename: path.basename(destPath), path: destPath, sizeBytes: stat.size });
        return;
      }

      await pipeline(ffmpegProcess.stdout, res);
      return;
    }

    res.status(500).json({ error: 'Unable to stream media.' });
  } catch (err) {
    const mapped = mapYtdlError(err);
    res.status(mapped.status).json({ error: mapped.message });
    cleanup();
  }
});

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
