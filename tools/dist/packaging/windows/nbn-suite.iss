#ifndef AppVersion
  #error AppVersion is required.
#endif
#ifndef SourceDir
  #error SourceDir is required.
#endif
#ifndef OutputDir
  #error OutputDir is required.
#endif
#ifndef OutputBaseFilename
  #error OutputBaseFilename is required.
#endif
#ifndef InstallDirName
  #error InstallDirName is required.
#endif

#define EnvironmentKey "SYSTEM\CurrentControlSet\Control\Session Manager\Environment"

[Setup]
AppId={{0CBBE7D3-9BE6-4F55-B39A-7935FF2D7F01}
AppName=NBN
AppVersion={#AppVersion}
DefaultDirName={autopf}\{#InstallDirName}
OutputDir={#OutputDir}
OutputBaseFilename={#OutputBaseFilename}
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
Compression=lzma2/ultra64
SolidCompression=yes
ChangesEnvironment=yes
PrivilegesRequired=admin
WizardStyle=modern
UninstallDisplayIcon={app}\apps\workbench\Nbn.Tools.Workbench.exe

[Tasks]
Name: "desktopicon"; Description: "Create a desktop shortcut for Workbench"; Flags: unchecked

[Files]
Source: "{#SourceDir}\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{autoprograms}\NBN Workbench"; Filename: "{app}\apps\workbench\Nbn.Tools.Workbench.exe"
Name: "{commondesktop}\NBN Workbench"; Filename: "{app}\apps\workbench\Nbn.Tools.Workbench.exe"; Tasks: desktopicon

[Registry]
Root: HKLM; Subkey: "{#EnvironmentKey}"; ValueType: expandsz; ValueName: "Path"; ValueData: "{olddata};{app}\bin"; Check: NeedsAddPath(ExpandConstant('{app}\bin')); Flags: preservestringtype

[Code]
function NeedsAddPath(Path: string): Boolean;
var
  Paths: string;
begin
  Result := True;
  if RegQueryStringValue(HKLM, '{#EnvironmentKey}', 'Path', Paths) then
  begin
    Result := Pos(';' + Uppercase(Path) + ';', ';' + Uppercase(Paths) + ';') = 0;
  end;
end;

procedure RemovePath(Path: string);
var
  Paths: string;
  NewPaths: string;
  Token: string;
  Separator: Integer;
begin
  if not RegQueryStringValue(HKLM, '{#EnvironmentKey}', 'Path', Paths) then
  begin
    exit;
  end;

  NewPaths := '';
  while Length(Paths) > 0 do
  begin
    Separator := Pos(';', Paths);
    if Separator = 0 then
    begin
      Token := Paths;
      Paths := '';
    end
    else
    begin
      Token := Copy(Paths, 1, Separator - 1);
      Delete(Paths, 1, Separator);
    end;

    if CompareText(Token, Path) <> 0 then
    begin
      if Length(NewPaths) > 0 then
      begin
        NewPaths := NewPaths + ';';
      end;
      NewPaths := NewPaths + Token;
    end;
  end;

  RegWriteExpandStringValue(HKLM, '{#EnvironmentKey}', 'Path', NewPaths);
end;

procedure CurUninstallStepChanged(CurUninstallStep: TUninstallStep);
begin
  if CurUninstallStep = usUninstall then
  begin
    RemovePath(ExpandConstant('{app}\bin'));
  end;
end;
