# Windows Remote Setup

This repo includes helper scripts for Windows laptop log collection and remote repo checks:

- Windows export fallback:
  `[export-windows-workbench-logs.ps1](/home/hailey/AI/NBNv2/tools/dev/export-windows-workbench-logs.ps1)`
- Ubuntu pull-over-SSH:
  `[pull-windows-workbench-logs.sh](/home/hailey/AI/NBNv2/tools/dev/pull-windows-workbench-logs.sh)`
- Ubuntu pull/build/test over SSH:
  `[pull-windows-build-test.sh](/home/hailey/AI/NBNv2/tools/dev/pull-windows-build-test.sh)`

## Windows Setup

Run these commands in an elevated PowerShell window on the laptop once:

```powershell
Add-WindowsCapability -Online -Name OpenSSH.Server~~~~0.0.1.0
Start-Service sshd
Set-Service -Name sshd -StartupType Automatic

if (-not (Get-NetFirewallRule -Name sshd -ErrorAction SilentlyContinue)) {
    New-NetFirewallRule `
        -Name sshd `
        -DisplayName "OpenSSH Server (sshd)" `
        -Enabled True `
        -Direction Inbound `
        -Protocol TCP `
        -Action Allow `
        -LocalPort 22
}
```

Verify that SSH is listening:

```powershell
Get-Service sshd
Get-NetTCPConnection -LocalPort 22 -State Listen
Test-NetConnection -ComputerName localhost -Port 22
```

If `Add-WindowsCapability` fails with `0x800f0993`, install `OpenSSH Server` through `Settings > System > Optional Features`, or allow optional feature downloads from Windows Update and retry.

## Key Auth

If the Windows account is in `Administrators`, OpenSSH expects the key in the shared administrators file.

Run this in elevated PowerShell on the laptop, replacing the key with the Ubuntu machine's public key:

```powershell
$key = 'ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIMFDhlxuCD+vV8D7RTv/C8kgRC0/RWdE3eylGUXd4mx1 hailey@hailey-ubu'

New-Item -ItemType Directory -Force -Path $env:ProgramData\ssh | Out-Null
Set-Content -Path $env:ProgramData\ssh\administrators_authorized_keys -Value $key -Encoding ascii
icacls.exe "$env:ProgramData\ssh\administrators_authorized_keys" /inheritance:r /grant "Administrators:F" /grant "SYSTEM:F"
Restart-Service sshd
Get-Content $env:ProgramData\ssh\administrators_authorized_keys
```

If the account is not in `Administrators`, use `$HOME\.ssh\authorized_keys` instead.

Verify key auth from Ubuntu:

```bash
ssh -o PreferredAuthentications=publickey -o PasswordAuthentication=no -o IdentitiesOnly=yes -i ~/.ssh/id_ed25519 -o StrictHostKeyChecking=accept-new Haile@192.168.0.103 exit
```

## Usage

On the laptop, fallback local export:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File tools/dev/export-windows-workbench-logs.ps1
```

That refreshes:

```text
%USERPROFILE%\Downloads\NBNLogs
```

From the Ubuntu machine, direct pull:

```bash
bash tools/dev/pull-windows-workbench-logs.sh --host 192.168.0.103 --user Haile
```

That pulls:

```text
%LOCALAPPDATA%\Nbn.Workbench\logs
```

into:

```text
~/Downloads/NBNLogs
```

From the Ubuntu machine, remote repo sync/build/test:

```bash
bash tools/dev/pull-windows-build-test.sh --host 192.168.0.103 --user Haile
```

Override the Windows repo path if needed:

```bash
bash tools/dev/pull-windows-build-test.sh \
  --host 192.168.0.103 \
  --user Haile \
  --repo-path 'C:\Users\Haile\OneDrive\Documents\NBNv2'
```
