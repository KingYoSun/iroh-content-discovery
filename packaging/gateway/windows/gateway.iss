#ifndef AppVersion
  #error AppVersion must be supplied by the release build
#endif
#ifndef BuildDir
  #define BuildDir "..\..\..\dist\gateway\x86_64-pc-windows-msvc"
#endif

[Setup]
AppId={{C56DC14F-B910-47E2-ACB3-908566548C46}
AppName=Iroh Link Gateway
AppVersion={#AppVersion}
AppPublisher=n0-computer
AppPublisherURL=https://github.com/n0-computer/iroh-content-discovery
DefaultDirName={localappdata}\Programs\Iroh Link Gateway
DefaultGroupName=Iroh Link Gateway
PrivilegesRequired=lowest
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
MinVersion=10.0
OutputDir=..\..\..\dist
OutputBaseFilename=iroh-link-gateway-{#AppVersion}-windows-x64-setup
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
SetupIconFile=iroh-link-gateway.ico
UninstallDisplayIcon={app}\iroh-link-gateway.ico
CloseApplications=yes
RestartApplications=no
SetupLogging=yes

[Files]
Source: "{#BuildDir}\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs
Source: "iroh-link-gateway.ico"; DestDir: "{app}"; Flags: ignoreversion

[Registry]
Root: HKCU; Subkey: "Software\Microsoft\Windows\CurrentVersion\Run"; ValueType: string; ValueName: "Iroh Link Gateway"; ValueData: """{app}\iroh-link-gateway-background.exe"""; Flags: uninsdeletevalue

[Icons]
Name: "{group}\Install browser extensions"; Filename: "{app}\extensions\Install extensions.html"
Name: "{group}\Start gateway"; Filename: "{app}\iroh-link-gateway-background.exe"; WorkingDir: "{app}"; IconFilename: "{app}\iroh-link-gateway.ico"
Name: "{group}\Stop gateway"; Filename: "{app}\iroh-link-gateway-background.exe"; Parameters: "stop"; WorkingDir: "{app}"; IconFilename: "{app}\iroh-link-gateway.ico"
Name: "{group}\Uninstall Iroh Link Gateway"; Filename: "{uninstallexe}"

[Run]
Filename: "{app}\extensions\Install extensions.html"; Description: "Show browser extension installation instructions"; Flags: shellexec nowait postinstall skipifsilent

[Code]
function StopGateway(): Boolean;
var
  Code: Integer;
  Helper: String;
begin
  Helper := ExpandConstant('{app}\iroh-link-gateway-background.exe');
  Result := True;
  if FileExists(Helper) then
    Result := Exec(Helper, 'stop', '', SW_HIDE, ewWaitUntilTerminated, Code) and (Code = 0);
end;

function PrepareToInstall(var NeedsRestart: Boolean): String;
begin
  Result := '';
  if not StopGateway() then
    Result := 'The gateway could not be stopped. Close it and retry. Details are in %LOCALAPPDATA%\iroh-link-gateway\launcher.log.';
end;

function InitializeUninstall(): Boolean;
begin
  Result := StopGateway();
  if not Result then
    SuppressibleMsgBox('The gateway could not be stopped. Close it and retry uninstalling. Your data has not been removed.', mbError, MB_OK, IDOK);
end;

procedure CurStepChanged(CurStep: TSetupStep);
var
  Code: Integer;
begin
  if CurStep = ssPostInstall then
  begin
    if not Exec(ExpandConstant('{app}\iroh-link-gateway-background.exe'), '', '', SW_HIDE, ewWaitUntilTerminated, Code) or (Code <> 0) then
      SuppressibleMsgBox('Iroh Link Gateway is installed, but gateway startup failed (port 45475 may already be in use). See %LOCALAPPDATA%\iroh-link-gateway\launcher.log, then run iroh-link-gateway-background.exe to retry.', mbError, MB_OK, IDOK);
  end;
end;
