# Seen Archive

`Seen/` is the durable archive root for 所见. It is intentionally file-system driven:

- `records/<record_id>/record.json` stores one record's metadata.
- `records/<record_id>/content.md` stores the long-form text.
- `records/<record_id>/assets/` stores local assets owned by that record.
- `routes/<route_id>.json` stores an ordered `records[]` array.
- `indexes/*.json` are generated read indexes for the static website.

The website under `seen/` only reads these files. It does not create, edit, or delete archive files after deployment.

Use the local manager:

```powershell
node scripts/seen-archive.mjs init
node scripts/seen-archive.mjs create-route --name "东京夜行" --id route_tokyo_night_test
node scripts/seen-archive.mjs create-record --title "东京塔的雨夜" --id record_tokyo_tower_rain_test --route route_tokyo_night_test --position end
node scripts/seen-archive.mjs update-route --route route_tokyo_night_test --name "东京夜行新版"
node scripts/seen-archive.mjs update-route --route route_tokyo_night_test --records record_a_test,record_b_test
node scripts/seen-archive.mjs delete-route --route route_tokyo_night_test
node scripts/seen-archive.mjs rebuild-indexes
```
