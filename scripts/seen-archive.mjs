#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import process from "node:process";

const ROOT = process.cwd();
const SEEN_ROOT = path.join(ROOT, "seen");
const RECORDS_DIR = path.join(SEEN_ROOT, "records");
const ROUTES_DIR = path.join(SEEN_ROOT, "routes");
const ASSETS_DIR = path.join(SEEN_ROOT, "assets");
const CACHE_DIR = path.join(SEEN_ROOT, "cache");
const INDEXES_DIR = path.join(SEEN_ROOT, "indexes");

const emptyRecordRoute = { route_id: null, route_index: null };

function usage() {
  console.log(`
Seen archive manager

Commands:
  init
  rebuild-indexes
  create-record --title "Tokyo rain" [--id record_tokyo_rain] [--date 2026-05-16] [--tags rain,night] [--lat 35.6586] [--lng 139.7454] [--place "Japan / Tokyo"] [--route route_tokyo --position end]
  create-route --name "Tokyo Night" [--id route_tokyo_night] [--date 2026-05-16]
  update-route --route route_tokyo_night [--name "Tokyo Night"] [--date 2026-05-16] [--records record_a,record_b]
  add-record-to-route --record record_tokyo_rain --route route_tokyo_night [--position end|0]
  remove-record-from-route --record record_tokyo_rain --route route_tokyo_night
  delete-record --record record_tokyo_rain
  delete-route --route route_tokyo_night
`);
}

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith("--")) continue;
    const key = token.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith("--")) {
      args[key] = true;
    } else {
      args[key] = next;
      i += 1;
    }
  }
  return args;
}

function ensureDirs() {
  [SEEN_ROOT, RECORDS_DIR, ROUTES_DIR, ASSETS_DIR, CACHE_DIR, INDEXES_DIR].forEach((dir) => {
    fs.mkdirSync(dir, { recursive: true });
  });
}

function nowIso() {
  return new Date().toISOString();
}

function slug(value, fallback = "seen") {
  return String(value || fallback)
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9\u4e00-\u9fa5]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 56) || fallback;
}

function readJson(file, fallback = null) {
  if (!fs.existsSync(file)) return fallback;
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function writeJson(file, data) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

function writeTextIfMissing(file, content) {
  if (fs.existsSync(file)) return;
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, content, "utf8");
}

function recordDir(recordId) {
  return path.join(RECORDS_DIR, recordId);
}

function recordFile(recordId) {
  return path.join(recordDir(recordId), "record.json");
}

function routeFile(routeId) {
  return path.join(ROUTES_DIR, `${routeId}.json`);
}

function assertRecordExists(recordId) {
  const file = recordFile(recordId);
  if (!fs.existsSync(file)) throw new Error(`Record not found: ${recordId}`);
  return file;
}

function assertRouteExists(routeId) {
  const file = routeFile(routeId);
  if (!fs.existsSync(file)) throw new Error(`Route not found: ${routeId}`);
  return file;
}

function listRecordIds() {
  if (!fs.existsSync(RECORDS_DIR)) return [];
  return fs.readdirSync(RECORDS_DIR, { withFileTypes: true })
    .filter((entry) => entry.isDirectory() && fs.existsSync(recordFile(entry.name)))
    .map((entry) => entry.name)
    .sort();
}

function listRouteIds() {
  if (!fs.existsSync(ROUTES_DIR)) return [];
  return fs.readdirSync(ROUTES_DIR, { withFileTypes: true })
    .filter((entry) => entry.isFile() && entry.name.endsWith(".json"))
    .map((entry) => path.basename(entry.name, ".json"))
    .sort();
}

function loadRecord(recordId) {
  return readJson(assertRecordExists(recordId));
}

function saveRecord(record) {
  writeJson(recordFile(record.id), record);
}

function loadRoute(routeId) {
  return readJson(assertRouteExists(routeId));
}

function saveRoute(route) {
  writeJson(routeFile(route.id), route);
}

function normalizePosition(position, length) {
  if (position === undefined || position === null || position === "" || position === "end") return length;
  const index = Number(position);
  if (!Number.isInteger(index) || index < 0 || index > length) {
    throw new Error(`Invalid route position: ${position}`);
  }
  return index;
}

function syncRoute(routeId) {
  const route = loadRoute(routeId);
  route.records = [...new Set(route.records || [])];
  route.records.forEach((recordId, index) => {
    const file = recordFile(recordId);
    if (!fs.existsSync(file)) return;
    const record = readJson(file);
    record.route_id = route.id;
    record.route_index = index;
    record.updated_at = nowIso();
    saveRecord(record);
  });
  route.updated_at = nowIso();
  saveRoute(route);
}

function detachRecordFromRoute(recordId, routeId) {
  const route = loadRoute(routeId);
  route.records = (route.records || []).filter((id) => id !== recordId);
  route.updated_at = nowIso();
  saveRoute(route);

  const record = loadRecord(recordId);
  if (record.route_id === routeId) {
    Object.assign(record, emptyRecordRoute, { updated_at: nowIso() });
    saveRecord(record);
  }
  syncRoute(routeId);
}

function rebuildIndexes() {
  ensureDirs();
  const records = listRecordIds().map((recordId) => {
    const record = loadRecord(recordId);
    return {
      id: record.id,
      title: record.title,
      path: `records/${record.id}/record.json`,
      content_path: `records/${record.id}/content.md`,
      route_id: record.route_id ?? null,
      route_index: record.route_index ?? null,
      tags: record.tags || [],
      location: record.location || {},
      cover: record.cover ? `records/${record.id}/${record.cover}` : null,
      created_at: record.created_at || null,
      updated_at: record.updated_at || null,
      favorite: Boolean(record.favorite),
      archived: Boolean(record.archived)
    };
  });

  const routes = listRouteIds().map((routeId) => {
    const route = loadRoute(routeId);
    return {
      id: route.id,
      name: route.name,
      path: `routes/${route.id}.json`,
      total_count: (route.records || []).length,
      date: route.date || null,
      created_at: route.created_at || null,
      updated_at: route.updated_at || null
    };
  });

  const updatedAt = nowIso();
  writeJson(path.join(INDEXES_DIR, "records_index.json"), {
    schema_version: 1,
    updated_at: updatedAt,
    records
  });
  writeJson(path.join(INDEXES_DIR, "routes_index.json"), {
    schema_version: 1,
    updated_at: updatedAt,
    routes
  });
  writeJson(path.join(INDEXES_DIR, "archive_index.json"), {
    schema_version: 1,
    root: "Seen",
    updated_at: updatedAt,
    records_count: records.length,
    routes_count: routes.length
  });
}

function createRoute(args) {
  ensureDirs();
  if (!args.name) throw new Error("--name is required");
  const id = args.id || `route_${slug(args.name)}`;
  const file = routeFile(id);
  if (fs.existsSync(file)) throw new Error(`Route already exists: ${id}`);
  const stamp = nowIso();
  saveRoute({
    id,
    name: args.name,
    date: args.date || stamp.slice(0, 10),
    created_at: stamp,
    updated_at: stamp,
    records: []
  });
  rebuildIndexes();
  console.log(`Created route: ${id}`);
}

function updateRoute(args) {
  ensureDirs();
  if (!args.route) throw new Error("--route is required");
  const route = loadRoute(args.route);
  const previousRecordIds = new Set(route.records || []);
  if (args.name) route.name = args.name;
  if (args.date) route.date = args.date;
  if (args.records !== undefined) {
    const nextRecordIds = String(args.records)
      .split(",")
      .map((id) => id.trim())
      .filter(Boolean);
    nextRecordIds.forEach(assertRecordExists);
    route.records = [...new Set(nextRecordIds)];
  }
  route.updated_at = nowIso();
  saveRoute(route);

  const nextRecordIds = new Set(route.records || []);
  for (const recordId of previousRecordIds) {
    if (nextRecordIds.has(recordId) || !fs.existsSync(recordFile(recordId))) continue;
    const record = loadRecord(recordId);
    if (record.route_id === route.id) {
      Object.assign(record, emptyRecordRoute, { updated_at: nowIso() });
      saveRecord(record);
    }
  }
  syncRoute(route.id);
  rebuildIndexes();
  console.log(`Updated route: ${route.id}`);
}

function createRecord(args) {
  ensureDirs();
  if (!args.title) throw new Error("--title is required");
  const id = args.id || `record_${slug(args.title)}`;
  const dir = recordDir(id);
  const file = recordFile(id);
  if (fs.existsSync(file)) throw new Error(`Record already exists: ${id}`);
  fs.mkdirSync(path.join(dir, "assets"), { recursive: true });
  const stamp = nowIso();
  const record = {
    id,
    title: args.title,
    created_at: stamp,
    updated_at: stamp,
    route_id: null,
    route_index: null,
    tags: args.tags ? String(args.tags).split(",").map((tag) => tag.trim()).filter(Boolean) : [],
    location: {
      place: args.place || "",
      lat: args.lat === undefined ? null : Number(args.lat),
      lng: args.lng === undefined ? null : Number(args.lng)
    },
    cover: "cover.jpg",
    assets: [],
    favorite: false,
    archived: false,
    type: args.type || "note",
    date: args.date || stamp.slice(0, 10),
    place: args.place || "",
    lat: args.lat === undefined ? null : Number(args.lat),
    lng: args.lng === undefined ? null : Number(args.lng)
  };
  saveRecord(record);
  writeTextIfMissing(path.join(dir, "content.md"), `# ${args.title}\n\n`);

  if (args.route) {
    addRecordToRoute({ record: id, route: args.route, position: args.position || "end" });
  } else {
    rebuildIndexes();
  }
  console.log(`Created record: ${id}`);
}

function addRecordToRoute(args) {
  ensureDirs();
  if (!args.record || !args.route) throw new Error("--record and --route are required");
  assertRecordExists(args.record);
  const route = loadRoute(args.route);
  route.records = (route.records || []).filter((id) => id !== args.record);
  route.records.splice(normalizePosition(args.position, route.records.length), 0, args.record);
  route.updated_at = nowIso();
  saveRoute(route);

  for (const routeId of listRouteIds()) {
    if (routeId !== args.route) {
      const other = loadRoute(routeId);
      if ((other.records || []).includes(args.record)) detachRecordFromRoute(args.record, routeId);
    }
  }
  syncRoute(args.route);
  rebuildIndexes();
  console.log(`Added ${args.record} to ${args.route}`);
}

function removeRecordFromRoute(args) {
  ensureDirs();
  if (!args.record || !args.route) throw new Error("--record and --route are required");
  detachRecordFromRoute(args.record, args.route);
  rebuildIndexes();
  console.log(`Removed ${args.record} from ${args.route}`);
}

function deleteRecord(args) {
  ensureDirs();
  if (!args.record) throw new Error("--record is required");
  assertRecordExists(args.record);
  for (const routeId of listRouteIds()) {
    const route = loadRoute(routeId);
    if (!(route.records || []).includes(args.record)) continue;
    route.records = route.records.filter((id) => id !== args.record);
    route.updated_at = nowIso();
    saveRoute(route);
    syncRoute(routeId);
  }
  fs.rmSync(recordDir(args.record), { recursive: true, force: true });
  rebuildIndexes();
  console.log(`Deleted record and removed route references: ${args.record}`);
}

function deleteRoute(args) {
  ensureDirs();
  if (!args.route) throw new Error("--route is required");
  const route = loadRoute(args.route);
  for (const recordId of route.records || []) {
    const file = recordFile(recordId);
    if (!fs.existsSync(file)) continue;
    const record = readJson(file);
    if (record.route_id === route.id) {
      Object.assign(record, emptyRecordRoute, { updated_at: nowIso() });
      saveRecord(record);
    }
  }
  fs.unlinkSync(routeFile(args.route));
  rebuildIndexes();
  console.log(`Deleted route and detached records: ${args.route}`);
}

function initArchive() {
  ensureDirs();
  rebuildIndexes();
  console.log("Initialized Seen archive.");
}

try {
  const [command, ...rest] = process.argv.slice(2);
  const args = parseArgs(rest);
  if (!command || command === "help" || command === "--help") {
    usage();
    process.exit(command ? 0 : 1);
  }
  if (command === "init") initArchive();
  else if (command === "rebuild-indexes") rebuildIndexes();
  else if (command === "create-record") createRecord(args);
  else if (command === "create-route") createRoute(args);
  else if (command === "update-route") updateRoute(args);
  else if (command === "add-record-to-route") addRecordToRoute(args);
  else if (command === "remove-record-from-route") removeRecordFromRoute(args);
  else if (command === "delete-record") deleteRecord(args);
  else if (command === "delete-route") deleteRoute(args);
  else throw new Error(`Unknown command: ${command}`);
} catch (error) {
  console.error(error.message);
  process.exit(1);
}
