; SonicNet installer (Inno Setup 6.3+). Built by packaging/windows/build.ps1, which passes:
;   /DAppVersion=0.1.0         display version (may carry a pre-release suffix)
;   /DAppVersionNumeric=0.1.0  numeric x.y.z for the setup exe's version resource
;   /DSourceExe=<path>         the release sonicnet.exe; sonicnet-system.exe and wintun.dll
;                              come from the same folder (cargo build puts them there)
;   /DSign + /Ssonic=<cmd>     only when signing (signs setup and uninstaller)
; Building by hand: ISCC /DAppVersion=0.1.0 /DAppVersionNumeric=0.1.0 /DSourceExe=..\..\target\release\sonicnet.exe sonicnet.iss

#ifndef AppVersion
  #error Pass /DAppVersion=x.y.z (build.ps1 does this)
#endif
#ifndef AppVersionNumeric
  #define AppVersionNumeric AppVersion
#endif
#ifndef SourceExe
  #define SourceExe "..\..\target\release\sonicnet.exe"
#endif

#define SourceDir ExtractFileDir(SourceExe)
#define AppName "SonicNet"
#define AppExe "sonicnet.exe"
#define Root "..\.."

[Setup]
; Never change AppId: it is how upgrades and the uninstaller find an existing install.
AppId={{C0362B9D-E07F-4688-A770-874CBF223599}
AppName={#AppName}
AppVersion={#AppVersion}
AppVerName={#AppName} {#AppVersion}
AppPublisher=The SonicNet authors
AppCopyright=Copyright (c) 2026 the SonicNet authors
VersionInfoVersion={#AppVersionNumeric}
VersionInfoProductName={#AppName}
VersionInfoDescription={#AppName} Setup

; Per-user by default (no UAC prompt, installs under %LOCALAPPDATA%\Programs); the first page
; offers "Install for all users", and /ALLUSERS or /CURRENTUSER pick on the command line.
; Every {auto*} constant and the HKA registry root follow that choice.
PrivilegesRequired=lowest
PrivilegesRequiredOverridesAllowed=dialog commandline
DefaultDirName={autopf}\{#AppName}
DefaultGroupName={#AppName}
DisableProgramGroupPage=yes
UsedUserAreasWarning=no

ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
MinVersion=10.0

SetupIconFile={#Root}\assets\sonicnet.ico
UninstallDisplayIcon={app}\{#AppExe}
UninstallDisplayName={#AppName}
WizardStyle=modern
; target\ is already git-ignored.
OutputDir={#Root}\target\installer
OutputBaseFilename=SonicNet-Setup-{#AppVersion}-x64
Compression=lzma2/max
SolidCompression=yes

; Upgrading over a running copy: ask Restart Manager to close it first.
CloseApplications=yes
; Lets Explorer pick up the URL protocol without a sign-out.
ChangesAssociations=yes

#ifdef Sign
SignTool=sonic
SignedUninstaller=yes
#endif

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked

[Files]
Source: "{#SourceExe}"; DestDir: "{app}"; DestName: "{#AppExe}"; Flags: ignoreversion
Source: "{#Root}\LICENSE"; DestDir: "{app}"; DestName: "LICENSE.txt"
Source: "{#Root}\NOTICE"; DestDir: "{app}"; DestName: "NOTICE.txt"
; System mode: the elevated helper, and Wintun's signed DLL (unmodified, used only through its
; wintun.h API, as its licence requires for redistribution) with that licence beside it.
Source: "{#SourceDir}\sonicnet-system.exe"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#SourceDir}\wintun.dll"; DestDir: "{app}"; Flags: ignoreversion
Source: "{#Root}\vendor\wintun\LICENSE.txt"; DestDir: "{app}"; DestName: "wintun-LICENSE.txt"

[Icons]
Name: "{autoprograms}\{#AppName}"; Filename: "{app}\{#AppExe}"
Name: "{autodesktop}\{#AppName}"; Filename: "{app}\{#AppExe}"; Tasks: desktopicon

[Registry]
; sonicnet:// URL protocol, so the browser extension and links can hand downloads to the app.
; HKA = HKCU\Software\Classes for a per-user install, HKLM\Software\Classes for all users.
; uninsdeletekey removes the whole key on uninstall.
Root: HKA; Subkey: "Software\Classes\sonicnet"; ValueType: string; ValueName: ""; ValueData: "URL:SonicNet Protocol"; Flags: uninsdeletekey
Root: HKA; Subkey: "Software\Classes\sonicnet"; ValueType: string; ValueName: "URL Protocol"; ValueData: ""
Root: HKA; Subkey: "Software\Classes\sonicnet\DefaultIcon"; ValueType: string; ValueName: ""; ValueData: """{app}\{#AppExe}"",0"
Root: HKA; Subkey: "Software\Classes\sonicnet\shell\open\command"; ValueType: string; ValueName: ""; ValueData: """{app}\{#AppExe}"" ""%1"""

[Run]
Filename: "{app}\{#AppExe}"; Description: "{cm:LaunchProgram,{#AppName}}"; Flags: nowait postinstall skipifsilent
