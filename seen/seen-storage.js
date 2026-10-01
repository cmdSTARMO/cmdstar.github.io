/* Atomic IndexedDB snapshots with binary image storage. */
window.SeenStorage = (() => {
  let database, queue = Promise.resolve();
  function db() {
    return database ||= new Promise((resolve, reject) => {
      const r = indexedDB.open('seen-drafts', 1);
      r.onupgradeneeded = () => r.result.createObjectStore('snapshots');
      r.onsuccess = () => resolve(r.result);
      r.onerror = () => reject(r.error);
      r.onblocked = () => reject(new Error('请关闭其他 Seen 页面后重试'));
    });
  }
  async function transform(value, encode, memo = new Map()) {
    if (encode && typeof value === 'string' && value.startsWith('data:image/')) {
      if (!memo.has(value)) memo.set(value, fetch(value).then(r => r.blob()));
      return memo.get(value);
    }
    if (!encode && value instanceof Blob) return new Promise((resolve, reject) => {
      const r = new FileReader(); r.onload = () => resolve(r.result); r.onerror = () => reject(r.error); r.readAsDataURL(value);
    });
    if (Array.isArray(value)) return Promise.all(value.map(v => transform(v, encode, memo)));
    if (value && typeof value === 'object') return Object.fromEntries(await Promise.all(Object.entries(value).map(async ([k,v]) => [k, await transform(v, encode, memo)])));
    return value;
  }
  async function load() {
    const connection = await db();
    const value = await new Promise((resolve, reject) => {
      const r = connection.transaction('snapshots').objectStore('snapshots').get('current');
      r.onsuccess = () => resolve(r.result); r.onerror = () => reject(r.error);
    });
    return transform(value, false);
  }
  function save(snapshot) {
    const copy = structuredClone(snapshot);
    const operation = queue.catch(() => {}).then(async () => {
      const encoded = await transform(copy, true), connection = await db();
      await new Promise((resolve, reject) => {
        const tx = connection.transaction('snapshots', 'readwrite');
        tx.objectStore('snapshots').put(encoded, 'current');
        tx.oncomplete = resolve; tx.onerror = () => reject(tx.error); tx.onabort = () => reject(tx.error || new Error('草稿写入中断'));
      });
    });
    queue = operation; return operation;
  }
  return { load, save };
})();
