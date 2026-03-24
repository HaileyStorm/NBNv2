# Windows Log Pull Setup

This repo includes two helper scripts for laptop log collection:

- Windows export fallback:
  `[export-windows-workbench-logs.ps1](/home/hailey/AI/NBNv2/tools/dev/export-windows-workbench-logs.ps1)`
- Ubuntu pull-over-SSH:
  `[pull-windows-workbench-logs.sh](/home/hailey/AI/NBNv2/tools/dev/pull-windows-workbench-logs.sh)`

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
Test-NetConnection -ComputerName localhost -Port 22
```

## Optional Key Auth

From the Ubuntu machine, install your SSH public key on the laptop:

```bash
ssh-copy-id Haile@192.168.0.103
```

If `ssh-copy-id` is not available, append the Ubuntu machine's public key to:

```powershell
$HOME\.ssh\authorized_keys
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
