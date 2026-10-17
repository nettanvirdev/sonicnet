<#
.SYNOPSIS
    Builds the release sonicnet.exe and the SonicNet installer.

.DESCRIPTION
    1. Finds Inno Setup (and signtool when signing) first, so a missing tool fails in seconds,
       not after a release build.
    2. cargo build --release --locked -p sonicnet
    3. Signs sonicnet.exe when SONICNET_SIGN_CERT is set.
    4. Compiles packaging\windows\sonicnet.iss; with signing on, ISCC also signs the setup and
       uninstaller.
    Output: target\installer\SonicNet-Setup-<version>-x64.exe

    Signing (optional), via environment variables:
      SONICNET_SIGN_CERT       path to a .pfx code-signing certificate
      SONICNET_SIGN_PASSWORD   its password (may be empty)
      SONICNET_TIMESTAMP_URL   RFC 3161 timestamp server (default http://timestamp.digicert.com)
    Tool locations can be forced with ISCC and SIGNTOOL (full paths).

.PARAMETER SkipBuild
    Package the existing target\release\sonicnet.exe instead of building it.

.EXAMPLE
    pwsh packaging/windows/build.ps1
#>
[CmdletBinding()]
param([switch]$SkipBuild)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$root = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$iss = Join-Path $PSScriptRoot 'sonicnet.iss'

function Find-Tool([string]$envVar, [string]$command, [string[]]$candidates) {
    $forced = [Environment]::GetEnvironmentVariable($envVar)
    if ($forced) {
        if (Test-Path -LiteralPath $forced) { return $forced }
        throw "$envVar is set to '$forced', which does not exist."
    }
    $onPath = Get-Command $command -ErrorAction SilentlyContinue
    if ($onPath) { return $onPath.Source }
    foreach ($c in $candidates) {
        $hit = Get-Item -Path $c -ErrorAction SilentlyContinue |
            Sort-Object { $_.Directory.Parent.Name } -Descending | Select-Object -First 1
        if ($hit) { return $hit.FullName }
    }
    return $null
}

# --- 1. tools ------------------------------------------------------------------------------------
$iscc = Find-Tool 'ISCC' 'iscc.exe' @(
    "${env:ProgramFiles(x86)}\Inno Setup 6\ISCC.exe",
    "$env:ProgramFiles\Inno Setup 6\ISCC.exe",
    "$env:LOCALAPPDATA\Programs\Inno Setup 6\ISCC.exe"
)
if (-not $iscc) {
    throw @"
Inno Setup 6 (ISCC.exe) was not found, so the installer can't be built.
Install it with one of:
    winget install --id JRSoftware.InnoSetup -e
    choco install innosetup
or download it from https://jrsoftware.org/isdl.php, then run this script again.
(If it's installed somewhere unusual, set ISCC to the full path of ISCC.exe.)
"@
}
Write-Host "Inno Setup: $iscc"

$cert = $env:SONICNET_SIGN_CERT
$signtool = $null
if ($cert) {
    if (-not (Test-Path -LiteralPath $cert)) { throw "SONICNET_SIGN_CERT points to '$cert', which does not exist." }
    $signtool = Find-Tool 'SIGNTOOL' 'signtool.exe' @(
        "${env:ProgramFiles(x86)}\Windows Kits\10\bin\*\x64\signtool.exe"
    )
    if (-not $signtool) {
        throw "SONICNET_SIGN_CERT is set but signtool.exe was not found. Install the Windows 10/11 SDK (signing tools) or set SIGNTOOL to its full path."
    }
    Write-Host "Signing with $cert via $signtool"
} else {
    Write-Host 'SONICNET_SIGN_CERT not set: building unsigned.'
}
$timestamp = if ($env:SONICNET_TIMESTAMP_URL) { $env:SONICNET_TIMESTAMP_URL } else { 'http://timestamp.digicert.com' }
# Windows PowerShell drops empty arguments, so /p is left out entirely when there's no password.
$password = $env:SONICNET_SIGN_PASSWORD

# --- 2. build ------------------------------------------------------------------------------------
Push-Location $root
try {
    $meta = cargo metadata --no-deps --format-version 1 --locked | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) { throw 'cargo metadata failed.' }
    $version = ($meta.packages | Where-Object name -EQ 'sonicnet').version
    if (-not $version) { throw "No 'sonicnet' package in the workspace." }
    # Windows version resources take numbers only: 0.2.0-rc.1 -> 0.2.0.
    $numeric = ($version -split '[-+]')[0]
    $targetDir = $meta.target_directory
    $exe = Join-Path $targetDir 'release\sonicnet.exe'

    if (-not $SkipBuild) {
        cargo build --release --locked -p sonicnet
        if ($LASTEXITCODE -ne 0) { throw 'cargo build failed.' }
    }
    if (-not (Test-Path -LiteralPath $exe)) { throw "$exe not found; run without -SkipBuild." }

    # --- 3. sign the app ---------------------------------------------------------------------------
    if ($cert) {
        $pw = if ($password) { @('/p', $password) } else { @() }
        & $signtool sign /fd SHA256 /td SHA256 /tr $timestamp /f $cert @pw $exe
        if ($LASTEXITCODE -ne 0) { throw 'Signing sonicnet.exe failed.' }
    }

    # --- 4. installer --------------------------------------------------------------------------------
    $isccArgs = @("/DAppVersion=$version", "/DAppVersionNumeric=$numeric", "/DSourceExe=$exe")
    if ($cert) {
        # ISCC runs this command for the setup and the uninstaller. In it $f is the file, $q a
        # double quote and $$ a dollar; $q avoids passing literal quotes through PowerShell.
        $quote = { param($s) '$q' + ($s -replace '\$', '$$$$') + '$q' }
        $cmd = @((& $quote $signtool), 'sign', '/fd', 'SHA256', '/td', 'SHA256', '/tr', $timestamp,
            '/f', (& $quote $cert)) -join ' '
        if ($password) { $cmd += ' /p ' + (& $quote $password) }
        $cmd += ' $f'
        $isccArgs += @('/DSign', "/Ssonic=$cmd")
    }
    & $iscc @isccArgs $iss
    if ($LASTEXITCODE -ne 0) { throw 'Inno Setup failed to compile the installer.' }

    $setup = Join-Path $root "target\installer\SonicNet-Setup-$version-x64.exe"
    Write-Host "`nInstaller: $setup"
} finally {
    Pop-Location
}
