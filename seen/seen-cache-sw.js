const CACHE_NAME = 'seen-image-cache-v2';
const MAX_ENTRIES = 120, MAX_IMAGE_BYTES = 2 * 1024 * 1024;
let writes = Promise.resolve();
self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', event => event.waitUntil((async () => {
  const keys = await caches.keys();
  await Promise.all(keys.filter(k => k.startsWith('seen-image-cache-') && k !== CACHE_NAME).map(k => caches.delete(k)));
  await self.clients.claim();
})()));
self.addEventListener('fetch', event => {
  const request = event.request, url = new URL(request.url);
  if (request.method !== 'GET' || url.origin !== self.location.origin || request.destination !== 'image' || !url.pathname.startsWith('/seen/records/')) return;
  event.respondWith((async () => {
    const cache = await caches.open(CACHE_NAME);
    try {
      // Normal HTTP cache revalidation; external map tiles are never intercepted.
      const response = await fetch(request);
      if (response.ok && response.headers.get('content-type')?.startsWith('image/')) {
        const copy = response.clone();
        const write = writes.catch(() => {}).then(async () => {
          const blob = await copy.blob(); if (blob.size > MAX_IMAGE_BYTES) return;
          await cache.delete(request);
          await cache.put(request, new Response(blob, { status: response.status, headers: response.headers }));
          const keys = await cache.keys();
          await Promise.all(keys.slice(0, Math.max(0, keys.length - MAX_ENTRIES)).map(k => cache.delete(k)));
        });
        writes = write; event.waitUntil(write.catch(() => {}));
      }
      return response;
    } catch (error) { const cached = await cache.match(request); if (cached) return cached; throw error; }
  })());
});
