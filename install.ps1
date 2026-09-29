# Install go-cdc on Windows (build from source).
#
# Usage:
#   irm https://raw.githubusercontent.com/PyRSA/go-cdc/main/install.ps1 | iex
#   ./install.ps1
#   ./install.ps1 -BinDir "$env:USERPROFILE\bin"
#   ./install.ps1 -BinDir (Join-Path (go env GOPATH) "bin")
#   ./install.ps1 -Version v0.1.0

[CmdletBinding()]
param(
    [string]$BinDir = "",
    [string]$Version = "",
    [string]$RepoUrl = "https://github.com/PyRSA/go-cdc.git",
    [string]$Ref = "main"
)

$ErrorActionPreference = "Stop"
$BinaryName = "go-cdc.exe"

function Write-Step([string]$Message) {
    Write-Host "+ $Message"
}

function Get-DefaultBinDir {
    if ($BinDir) { return $BinDir }
    return (Join-Path $env:USERPROFILE "bin")
}

function Test-IsLocalRepo([string]$Path) {
    return (Test-Path (Join-Path $Path "go.mod")) -and
        (Test-Path (Join-Path $Path "Makefile")) -and
        (Test-Path (Join-Path $Path "cmd\go-cdc"))
}

function Get-SourceRoot {
    if ($PSScriptRoot -and (Test-IsLocalRepo $PSScriptRoot)) {
        Write-Step "using local repository $PSScriptRoot"
        return @{ Root = $PSScriptRoot; Cleanup = $null }
    }

    if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
        throw "git is required to clone go-cdc"
    }
    if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
        throw "go is required to build go-cdc"
    }

    $tmp = Join-Path ([System.IO.Path]::GetTempPath()) ("go-cdc-install-" + [guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Path $tmp | Out-Null
    $targetRef = if ($Version) { $Version } else { $Ref }
    Write-Step "cloning $RepoUrl (ref=$targetRef) into $tmp"
    try {
        git clone --depth 1 --branch $targetRef $RepoUrl (Join-Path $tmp "go-cdc")
    } catch {
        git clone $RepoUrl (Join-Path $tmp "go-cdc")
        git -C (Join-Path $tmp "go-cdc") checkout $targetRef
    }
    return @{ Root = (Join-Path $tmp "go-cdc"); Cleanup = $tmp }
}

function Ensure-UserPath([string]$Dir) {
    $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
    if (-not $userPath) { $userPath = "" }
    $parts = $userPath -split ";" | Where-Object { $_ -ne "" }
    if ($parts -contains $Dir) {
        Write-Step "user PATH already contains $Dir"
        return
    }
    $newPath = if ($userPath) { "$Dir;$userPath" } else { $Dir }
    [Environment]::SetEnvironmentVariable("Path", $newPath, "User")
    $env:Path = "$Dir;$env:Path"
    Write-Step "added $Dir to user PATH (new terminals will pick it up)"
}

$destDir = Get-DefaultBinDir
$src = Get-SourceRoot
try {
    if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
        throw "go is required to build go-cdc (see https://go.dev/dl/)"
    }

    Write-Step "building go-cdc in $($src.Root)"
    Push-Location $src.Root
    try {
        New-Item -ItemType Directory -Force -Path "bin" | Out-Null
        & go build -o (Join-Path "bin" $BinaryName) ./cmd/go-cdc
        if ($LASTEXITCODE -ne 0) { throw "go build failed" }
    } finally {
        Pop-Location
    }

    $built = Join-Path $src.Root (Join-Path "bin" $BinaryName)
    if (-not (Test-Path $built)) { throw "build did not produce $built" }

    New-Item -ItemType Directory -Force -Path $destDir | Out-Null
    $dest = Join-Path $destDir $BinaryName
    Copy-Item -Force $built $dest
    Write-Step "installed $dest"

    Ensure-UserPath $destDir
    Write-Step "done. verify: & `"$dest`""
} finally {
    if ($src.Cleanup -and (Test-Path $src.Cleanup)) {
        Remove-Item -Recurse -Force $src.Cleanup
    }
}
