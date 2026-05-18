$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$recordsRoot = Resolve-Path (Join-Path $PSScriptRoot "records")
if (-not ($recordsRoot.Path.StartsWith($repoRoot.Path))) {
  throw "records path escaped workspace"
}

$migrated = 0

foreach ($recordDir in Get-ChildItem -LiteralPath $recordsRoot.Path -Directory) {
  $jsonPath = Join-Path $recordDir.FullName "record.json"
  if (-not (Test-Path -LiteralPath $jsonPath)) { continue }

  $json = Get-Content -LiteralPath $jsonPath -Raw | ConvertFrom-Json
  $sources = New-Object System.Collections.Generic.List[string]
  $seen = @{}

  function Add-ImageSource {
    param($RecordDir, $Seen, $Sources, [string]$Path)
    if ([string]::IsNullOrWhiteSpace($Path)) { return }
    $clean = $Path -replace "\\", "/"
    $prefix = "records/$($RecordDir.Name)/"
    if ($clean.StartsWith($prefix)) { $clean = $clean.Substring($prefix.Length) }
    if ($clean.StartsWith("./")) { $clean = $clean.Substring(2) }
    if ($clean.StartsWith("/") -or $clean -match "^(https?:|data:|blob:)") { return }
    if (-not $Seen.ContainsKey($clean)) {
      $Seen[$clean] = $true
      $Sources.Add($clean)
    }
  }

  Add-ImageSource $recordDir $seen $sources $json.cover
  if ($json.assets) {
    foreach ($asset in $json.assets) {
      Add-ImageSource $recordDir $seen $sources $asset.path
    }
  }

  if ($sources.Count -eq 0) {
    $assetsDir = Join-Path $recordDir.FullName "assets"
    if (Test-Path -LiteralPath $assetsDir) {
      Get-ChildItem -LiteralPath $assetsDir -File -Filter *.jpg | Sort-Object Name | ForEach-Object {
        Add-ImageSource $recordDir $seen $sources "assets/$($_.Name)"
      }
    }
  }

  if ($sources.Count -eq 0) { continue }

  $assetsPath = Join-Path $recordDir.FullName "assets"
  $thumbsPath = Join-Path $recordDir.FullName "thumbs"
  New-Item -ItemType Directory -Force -Path $assetsPath | Out-Null
  New-Item -ItemType Directory -Force -Path $thumbsPath | Out-Null

  $tmpAssets = Join-Path $assetsPath "__migrate_tmp"
  $tmpThumbs = Join-Path $thumbsPath "__migrate_tmp"
  New-Item -ItemType Directory -Force -Path $tmpAssets | Out-Null
  New-Item -ItemType Directory -Force -Path $tmpThumbs | Out-Null

  $newAssets = @()
  for ($i = 0; $i -lt $sources.Count; $i++) {
    $srcRel = $sources[$i]
    $srcFile = Join-Path $recordDir.FullName ($srcRel -replace "/", [IO.Path]::DirectorySeparatorChar)
    if (-not (Test-Path -LiteralPath $srcFile)) { continue }

    $nextName = "image-{0}.jpg" -f ($i + 1)
    Move-Item -LiteralPath $srcFile -Destination (Join-Path $tmpAssets $nextName) -Force

    $thumbName = if ($srcRel -match "^cover\.(jpg|jpeg|png|webp)$") {
      "cover.jpg"
    } else {
      ([IO.Path]::GetFileNameWithoutExtension($srcRel) + ".jpg")
    }
    $thumbFile = Join-Path $thumbsPath $thumbName
    if (Test-Path -LiteralPath $thumbFile) {
      Move-Item -LiteralPath $thumbFile -Destination (Join-Path $tmpThumbs $nextName) -Force
    }

    $newAssets += [pscustomobject]@{ path = "assets/$nextName"; type = "image/jpeg" }
  }

  Get-ChildItem -LiteralPath $assetsPath -File -Filter *.jpg -ErrorAction SilentlyContinue | Remove-Item -Force
  Get-ChildItem -LiteralPath $thumbsPath -File -Filter *.jpg -ErrorAction SilentlyContinue | Remove-Item -Force
  Get-ChildItem -LiteralPath $tmpAssets -File | Sort-Object Name | Move-Item -Destination $assetsPath -Force
  Get-ChildItem -LiteralPath $tmpThumbs -File | Sort-Object Name | Move-Item -Destination $thumbsPath -Force
  Remove-Item -LiteralPath $tmpAssets -Recurse -Force
  Remove-Item -LiteralPath $tmpThumbs -Recurse -Force

  foreach ($legacyCover in @("cover.jpg", "cover.jpeg", "cover.png", "cover.webp")) {
    Remove-Item -LiteralPath (Join-Path $recordDir.FullName $legacyCover) -Force -ErrorAction SilentlyContinue
  }

  $json.PSObject.Properties.Remove("cover")
  $json.PSObject.Properties.Remove("image")
  $json.assets = $newAssets
  $json.updated_at = (Get-Date).ToUniversalTime().ToString("o")
  $json | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath $jsonPath -Encoding UTF8
  $migrated++
}

"migrated records: $migrated"
