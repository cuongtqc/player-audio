const express = require('express');
const path = require('path');
const fs = require('fs');
const { pipeline } = require('stream/promises');
const { spawn } = require('child_process');

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
  return String(name || '')
    .replace(/[<>:"/\\|?*]+/g, '')
    .replace(/\s+/g, ' ')
    .trim();
}

function parseQuality(value) {
  if (!value) return 'best';

  const normalized = String(value).toLowerCase();
  if (normalized === 'highest' || normalized === 'best') return 'best';
  if (normalized === 'lowest' || normalized === 'worst') return 'worst';

  const num = Number(value);
  if (Number.isFinite(num)) {
    if (num >= 720) return `bestvideo[height<=${num}]+bestaudio/best[height<=${num}]`;
    return `best[height<=${num}]`;
  }

  return 'best';
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
  } catch {
    return false;
  }
}

function parseRangeHeader(rangeHeader, size) {
  if (!rangeHeader || !size) return null;

  const match = /bytes=(\d+)-(\d*)/i.exec(rangeHeader);
  if (!match) return null;

  const start = Number(match[1]);
  const end = match[2] ? Number(match[2]) : size - 1;

  if (!Number.isFinite(start) || !Number.isFinite(end) || start > end || end >= size) {
    return null;
  }

  return { start, end };
}

function spawnYtDlp(args) {
  return spawn('yt-dlp', args, {
    stdio: ['ignore', 'pipe', 'pipe'],
  });
}

async function runYtDlpJson(url) {
  return new Promise((resolve, reject) => {
    const proc = spawnYtDlp([
      '--dump-single-json',
      '--no-warnings',
      '--no-playlist',
      url,
    ]);

    let stdout = '';
    let stderr = '';

    proc.stdout.on('data', (chunk) => {
      stdout += chunk.toString();
    });

    proc.stderr.on('data', (chunk) => {
      stderr += chunk.toString();
    });

    proc.on('error', (err) => reject(err));

    proc.on('close', (code) => {
      if (code !== 0) {
        reject(new Error(stderr || `yt-dlp exited with code ${code}`));
        return;
      }

      try {
        resolve(JSON.parse(stdout));
      } catch (err) {
        reject(new Error(`Invalid yt-dlp JSON output: ${err.message}`));
      }
    });
  });
}

function mapYtDlpError(err) {
  const message = String(err && err.message ? err.message : err).toLowerCase();

  if (message.includes('private video')) {
    return { status: 403, message: 'Video is private.' };
  }

  if (message.includes('sign in to confirm your age') || message.includes('age-restricted')) {
    return { status: 403, message: 'Video is age restricted.' };
  }

  if (message.includes('video unavailable') || message.includes('unavailable')) {
    return { status: 404, message: 'Video is unavailable.' };
  }

  if (message.includes('unsupported url')) {
    return { status: 400, message: 'Unsupported URL.' };
  }

  if (message.includes('live stream')) {
    return { status: 400, message: 'Livestreams are not supported.' };
  }

  if (message.includes('requested format is not available')) {
    return { status: 409, message: 'Requested quality is not available.' };
  }

  if (message.includes('not found')) {
    return { status: 404, message: 'Media not found.' };
  }

  return { status: 500, message: 'Failed to fetch media.' };
}

function buildOutputFilename({ type, title, filename, ext, mp3 }) {
  const safeBase = sanitizeFilename(filename) || sanitizeFilename(title) || 'youtube-media';
  const finalExt = mp3 ? 'mp3' : ext || (type === 'audio' ? 'm4a' : 'mp4');
  return safeBase.endsWith(`.${finalExt}`) ? safeBase : `${safeBase}.${finalExt}`;
}

function toAsciiHeaderFilename(value) {
  return String(value || 'file')
    .replace(/[\r\n"]/g, '')
    .replace(/[^\x20-\x7E]+/g, '_')
    .trim() || 'file';
}

function encodeRFC5987ValueChars(str) {
  return encodeURIComponent(String(str || 'file'))
    .replace(/['()]/g, escape)
    .replace(/\*/g, '%2A');
}

function makeContentDisposition(filename, isDownload) {
  const dispositionType = isDownload ? 'attachment' : 'inline';
  const asciiFallback = toAsciiHeaderFilename(filename);
  const utf8Name = encodeRFC5987ValueChars(filename);
  return `${dispositionType}; filename="${asciiFallback}"; filename*=UTF-8''${utf8Name}`;
}

function getAudioMime(metadata, mp3) {
  if (mp3) return 'audio/mpeg';

  const audioFormats = Array.isArray(metadata.formats)
    ? metadata.formats.filter((fmt) => fmt && fmt.vcodec === 'none')
    : [];

  const bestAudio = audioFormats.sort((a, b) => (b.abr || 0) - (a.abr || 0))[0];
  const ext = String((bestAudio && bestAudio.ext) || metadata.ext || '').toLowerCase();

  if (ext === 'm4a' || ext === 'mp4') return 'audio/mp4';
  if (ext === 'webm') return 'audio/webm';
  if (ext === 'ogg' || ext === 'opus') return 'audio/ogg';
  if (ext === 'wav') return 'audio/wav';
  if (ext === 'flac') return 'audio/flac';

  return 'application/octet-stream';
}

function selectBestAudioFormat(metadata) {
  const audioFormats = Array.isArray(metadata.formats)
    ? metadata.formats.filter((fmt) => fmt && fmt.vcodec === 'none')
    : [];

  if (!audioFormats.length) return null;

  return audioFormats.sort((a, b) => {
    const aScore = (a.abr || 0) * 1000000 + (a.filesize || a.filesize_approx || 0);
    const bScore = (b.abr || 0) * 1000000 + (b.filesize || b.filesize_approx || 0);
    return bScore - aScore;
  })[0];
}

function isSafePartialAudio(metadata, mp3) {
  if (mp3) return false;

  const bestAudio = selectBestAudioFormat(metadata);
  if (!bestAudio) return false;
  if (!bestAudio.url) return false;

  const size = Number(bestAudio.filesize || bestAudio.filesize_approx || 0);
  if (!Number.isFinite(size) || size <= 0) return false;

  const ext = String(bestAudio.ext || '').toLowerCase();
  return ['m4a', 'mp4', 'webm', 'ogg', 'opus'].includes(ext);
}

function buildSafeAudioArgs({ url, formatId, start, end, allowRange }) {
  const args = ['--no-warnings', '--no-playlist', '-f', String(formatId)];

  if (allowRange && Number.isFinite(start) && Number.isFinite(end)) {
    args.push('--downloader', 'http', '--downloader-args', `http:Range=bytes=${start}-${end}`);
  }

  args.push('-o', '-');
  args.push(url);
  return args;
}

function buildFallbackStreamArgs({ url, type, quality, mp3 }) {
  const args = ['--no-warnings', '--no-playlist'];

  if (type === 'audio') {
    args.push('-f', 'bestaudio/best');

    if (mp3) {
      args.push('--extract-audio', '--audio-format', 'mp3', '--audio-quality', '192K');
    }
  } else {
    const q = parseQuality(quality);
    if (q === 'best') args.push('-f', 'bestvideo*+bestaudio/best');
    else if (q === 'worst') args.push('-f', 'worst');
    else args.push('-f', q);

    args.push('--merge-output-format', 'mp4');
  }

  args.push('-o', '-');
  args.push(url);
  return args;
}

app.get('/api/media', async (req, res) => {
  const {
    url,
    mode = 'stream',
    type = 'video',
    quality = 'highest',
    downloadTarget = 'response',
    filename,
    format,
  } = req.query;

  if (!url || typeof url !== 'string' || !isValidYoutubeUrl(url)) {
    res.status(400).json({ error: 'Invalid YouTube URL.' });
    return;
  }

  const mediaType = type === 'audio' ? 'audio' : 'video';
  const isDownload = mode === 'download';
  const isDisk = isDownload && downloadTarget === 'disk';
  const wantMp3 = mediaType === 'audio' && String(format || '').toLowerCase() === 'mp3';

  let metadata;
  try {
    metadata = await runYtDlpJson(url);
  } catch (err) {
    const mapped = mapYtDlpError(err);
    res.status(mapped.status).json({ error: mapped.message, details: String(err.message || err) });
    return;
  }

  const responseFilename = buildOutputFilename({
    type: mediaType,
    title: metadata.title,
    filename,
    ext: metadata.ext,
    mp3: wantMp3,
  });

  const contentDisposition = makeContentDisposition(responseFilename, isDownload);

  const safePartial = !isDisk && !isDownload && mediaType === 'audio' && isSafePartialAudio(metadata, wantMp3);
  const selectedAudioFormat = safePartial ? selectBestAudioFormat(metadata) : null;
  const knownSize = safePartial && selectedAudioFormat
    ? Number(selectedAudioFormat.filesize || selectedAudioFormat.filesize_approx || 0)
    : null;
  const requestedRange = safePartial ? parseRangeHeader(req.headers.range, knownSize) : null;

  const contentType = mediaType === 'audio'
    ? getAudioMime(metadata, wantMp3)
    : 'video/mp4';

  const ytDlpArgs = safePartial && selectedAudioFormat
    ? buildSafeAudioArgs({
        url,
        formatId: selectedAudioFormat.format_id,
        start: requestedRange?.start,
        end: requestedRange?.end,
        allowRange: Boolean(requestedRange),
      })
    : buildFallbackStreamArgs({
        url,
        type: mediaType,
        quality,
        mp3: wantMp3,
      });

  const proc = spawnYtDlp(ytDlpArgs);
  let stderr = '';
  let responseStarted = false;
  let requestClosed = false;

  const cleanup = () => {
    requestClosed = true;
    if (!proc.killed) {
      proc.kill('SIGKILL');
    }
  };

  req.on('close', cleanup);
  res.on('close', cleanup);

  proc.stderr.on('data', (chunk) => {
    stderr += chunk.toString();
  });

  proc.on('error', (err) => {
    if (!res.headersSent) {
      const mapped = mapYtDlpError(err);
      res.status(mapped.status).json({ error: mapped.message, details: String(err.message || err) });
    }
  });

  try {
    if (isDisk) {
      await fs.promises.mkdir(DOWNLOAD_DIR, { recursive: true });
      const destPath = path.join(DOWNLOAD_DIR, responseFilename);
      await pipeline(proc.stdout, fs.createWriteStream(destPath));

      const exitCode = await new Promise((resolve) => proc.on('close', resolve));
      if (exitCode !== 0) {
        throw new Error(stderr || `yt-dlp exited with code ${exitCode}`);
      }

      const stat = await fs.promises.stat(destPath);
      res.json({ ok: true, filename: responseFilename, path: destPath, sizeBytes: stat.size });
      return;
    }

    res.setHeader('Content-Disposition', contentDisposition);
    res.setHeader('Cache-Control', 'no-store');
    res.setHeader('Content-Type', contentType);

    if (safePartial && knownSize) {
      res.setHeader('Accept-Ranges', 'bytes');

      if (requestedRange) {
        res.status(206);
        res.setHeader('Content-Range', `bytes ${requestedRange.start}-${requestedRange.end}/${knownSize}`);
        res.setHeader('Content-Length', String(requestedRange.end - requestedRange.start + 1));
      } else {
        res.setHeader('Content-Length', String(knownSize));
      }
    }

    responseStarted = true;
    await pipeline(proc.stdout, res);

    const exitCode = await new Promise((resolve) => proc.on('close', resolve));
    if (!requestClosed && exitCode !== 0) {
      throw new Error(stderr || `yt-dlp exited with code ${exitCode}`);
    }
  } catch (err) {
    cleanup();

    if (!responseStarted && !res.headersSent) {
      const mapped = mapYtDlpError(err);
      res.status(mapped.status).json({ error: mapped.message, details: String(err.message || err) });
      return;
    }

    if (!res.headersSent) {
      res.status(500).end();
    } else if (!res.writableEnded) {
      res.end();
    }
  }
});

app.get('/api/media/info', async (req, res) => {
  const { url, type = 'video', quality = 'highest', format } = req.query;

  if (!url || typeof url !== 'string' || !isValidYoutubeUrl(url)) {
    res.status(400).json({ error: 'Invalid YouTube URL.' });
    return;
  }

  try {
    const metadata = await runYtDlpJson(url);
    const mediaType = type === 'audio' ? 'audio' : 'video';
    const wantMp3 = mediaType === 'audio' && String(format || '').toLowerCase() === 'mp3';
    const outputFilename = buildOutputFilename({
      type: mediaType,
      title: metadata.title,
      filename: '',
      ext: metadata.ext,
      mp3: wantMp3,
    });

    const safePartial = mediaType === 'audio' && isSafePartialAudio(metadata, wantMp3);
    const bestAudio = safePartial ? selectBestAudioFormat(metadata) : null;

    res.json({
      ok: true,
      id: metadata.id,
      title: metadata.title,
      duration: metadata.duration,
      thumbnail: metadata.thumbnail,
      ext: metadata.ext,
      requestedType: mediaType,
      requestedQuality: quality,
      suggestedFilename: outputFilename,
      extractor: metadata.extractor,
      webpageUrl: metadata.webpage_url,
      safePartial,
      streamMime: mediaType === 'audio' ? getAudioMime(metadata, wantMp3) : 'video/mp4',
      safePartialSizeBytes: bestAudio ? Number(bestAudio.filesize || bestAudio.filesize_approx || 0) : null,
    });
  } catch (err) {
    const mapped = mapYtDlpError(err);
    res.status(mapped.status).json({ error: mapped.message, details: String(err.message || err) });
  }
});

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});

