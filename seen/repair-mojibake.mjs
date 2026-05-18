import fs from "node:fs";
import path from "node:path";

const decoder = new TextDecoder("windows-1252");
const utf8Fatal = new TextDecoder("utf-8", { fatal: true });
const reverse = new Map();

for (let byte = 0; byte <= 255; byte += 1) {
  reverse.set(decoder.decode(Uint8Array.of(byte)), byte);
}

const mojibakePattern = /[ÃÂ]|[äåæçèé][\u0080-\u00ff\u0152\u0153\u0160\u0161\u0178\u017D\u017E\u2018-\u201D\u2020-\u2026\u2030\u20AC\u2122]/;
const cjkPattern = /[\u3400-\u9fff]/g;
const badPattern = /[ÃÂ]|[\u0080-\u009f]|[åæçèéä][\u0080-\u00ff\u0152\u0153\u0160\u0161\u0178\u017D\u017E\u2018-\u201D\u2020-\u2026\u2030\u20AC\u2122]/g;

function bytesFromWindows1252Text(value) {
  const bytes = [];
  for (const char of value) {
    const code = char.codePointAt(0);
    if (code <= 255) {
      bytes.push(code);
      continue;
    }
    if (!reverse.has(char)) return null;
    bytes.push(reverse.get(char));
  }
  return Uint8Array.from(bytes);
}

function score(value) {
  const cjk = value.match(cjkPattern)?.length || 0;
  const bad = value.match(badPattern)?.length || 0;
  return cjk * 8 - bad * 10;
}

function repairString(value) {
  if (!mojibakePattern.test(value)) return value;
  let current = value;
  let currentScore = score(current);
  for (let i = 0; i < 4; i += 1) {
    const bytes = bytesFromWindows1252Text(current);
    if (!bytes) break;
    let next;
    try {
      next = utf8Fatal.decode(bytes);
    } catch {
      break;
    }
    const nextScore = score(next);
    if (next === current || nextScore < currentScore) break;
    current = next;
    currentScore = nextScore;
  }
  return current;
}

function repairJson(value) {
  if (typeof value === "string") return repairString(value);
  if (Array.isArray(value)) return value.map(repairJson);
  if (value && typeof value === "object") {
    for (const key of Object.keys(value)) value[key] = repairJson(value[key]);
  }
  return value;
}

function readText(file) {
  return fs.readFileSync(file, "utf8").replace(/^\uFEFF/, "");
}

function repairJsonFile(file) {
  const repaired = repairJson(JSON.parse(readText(file)));
  fs.writeFileSync(file, `${JSON.stringify(repaired, null, 2)}\n`, "utf8");
}

function repairTextFile(file) {
  const before = readText(file);
  const after = before.replace(/"([^"\\]*(?:\\.[^"\\]*)*)"/g, (match) => {
    try {
      return JSON.stringify(repairString(JSON.parse(match)));
    } catch {
      return match;
    }
  });
  fs.writeFileSync(file, after, "utf8");
}

const recordsRoot = path.join("seen", "records");
for (const dir of fs.readdirSync(recordsRoot)) {
  const jsonPath = path.join(recordsRoot, dir, "record.json");
  if (fs.existsSync(jsonPath)) repairJsonFile(jsonPath);
}

repairJsonFile(path.join("seen", "indexes", "records_index.json"));
repairTextFile(path.join("seen", "index.html"));

console.log("mojibake repaired");
