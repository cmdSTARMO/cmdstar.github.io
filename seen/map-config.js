// Public basemap configuration; OSM tiles use the browser HTTP cache only.
window.SEEN_MAP_CONFIG = {
  url: 'https://tile.openstreetmap.org/{z}/{x}/{y}.png',
  attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
  // Client-side night adaptation, not a separate OSM tile style.
  nightFilter: true,
  geocodeUrl: 'https://nominatim.openstreetmap.org'
};
