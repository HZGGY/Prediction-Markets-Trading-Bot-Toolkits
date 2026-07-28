// Post-build prerender. Renders the built SPA with a real headless browser and
// snapshots the fully-rendered HTML back into dist/index.html, so crawlers and
// social scrapers see real content instead of an empty <div id="root">.
//
// BEST-EFFORT BY DESIGN: any failure (no puppeteer, launch error, timeout) is
// caught, logged, and the script exits 0 leaving the normal SPA build in place —
// it can never break the deploy.
import { createServer } from 'node:http';
import { readFile, writeFile } from 'node:fs/promises';
import { join, extname, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = dirname(fileURLToPath(import.meta.url));
const DIST = join(ROOT, 'dist');
const BASE = process.env.VITE_BASE || '/';

const MIME = {
  '.html': 'text/html', '.js': 'text/javascript', '.mjs': 'text/javascript',
  '.css': 'text/css', '.svg': 'image/svg+xml', '.json': 'application/json',
  '.xml': 'application/xml', '.txt': 'text/plain', '.ico': 'image/x-icon',
  '.png': 'image/png', '.jpg': 'image/jpeg', '.webp': 'image/webp', '.woff2': 'font/woff2',
};

async function main() {
  let puppeteer;
  try {
    puppeteer = (await import('puppeteer')).default;
  } catch (e) {
    console.warn('[prerender] puppeteer unavailable — skipping (SPA build kept):', e.message);
    return;
  }

  // Static server that serves dist/ under the configured base path.
  const server = createServer(async (req, res) => {
    let p = decodeURIComponent((req.url || '/').split('?')[0]);
    if (BASE !== '/' && p.startsWith(BASE)) p = '/' + p.slice(BASE.length);
    if (p === '/' || p === '') p = '/index.html';
    try {
      const data = await readFile(join(DIST, p));
      res.writeHead(200, { 'content-type': MIME[extname(p)] || 'application/octet-stream' });
      res.end(data);
    } catch {
      try {
        const data = await readFile(join(DIST, 'index.html')); // SPA fallback
        res.writeHead(200, { 'content-type': 'text/html' });
        res.end(data);
      } catch {
        res.writeHead(404);
        res.end('not found');
      }
    }
  });
  await new Promise((resolve) => server.listen(0, resolve));
  const port = server.address().port;
  const url = `http://localhost:${port}${BASE}`;

  let browser;
  try {
    browser = await puppeteer.launch({ headless: true, args: ['--no-sandbox', '--disable-setuid-sandbox'] });
    const page = await browser.newPage();
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
    // Wait until the React app has rendered real content (footer contact section).
    await page.waitForSelector('#contact', { timeout: 20000 });
    const html = await page.content();
    if (!html.includes('id="contact"')) throw new Error('rendered HTML missing expected content');
    await writeFile(join(DIST, 'index.html'), html);
    console.log(`[prerender] snapshotted dist/index.html (${html.length} bytes)`);
  } finally {
    if (browser) await browser.close();
    server.close();
  }
}

main().catch((e) => {
  console.warn('[prerender] skipped due to error (SPA build kept):', e.message);
  process.exit(0);
});
