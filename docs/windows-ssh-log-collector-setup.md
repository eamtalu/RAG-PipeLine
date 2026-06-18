# Windows ⇄ Ubuntu SSH Setup for Remote Log Collection

This document records how the FastAPI log collector (running on Ubuntu) was wired up
to pull WMS/M3 log files from a Windows Server over SSH/SFTP using key-based auth.

It is intended as a **repeatable runbook** for onboarding additional Windows hosts.

---

## 1. Architecture / Roles

| Host | OS | Role |
|------|----|------|
| `192.168.0.142` | Ubuntu 24.04.4 LTS | FastAPI log collector (SSH/SFTP **client**) |
| `192.168.0.124` (`LAPTOP-DPJEJEU6`) | Windows 10 (Build 26200) | WMS log source (OpenSSH **server**) |

- **Service account on Windows:** `svc_logs` (non-admin local user, dedicated to log fetching)
- **Auth model:** Ubuntu authenticates to Windows with an **ed25519 key pair** — no passwords in the collector.
- **Log location on Windows:** `C:\BEC Logs\*.txt*` (matches `.txt`, `.txt1`, `.txt2`, … rolled files)

> Replace IPs / hostnames / paths with the values for each new host you onboard.

---

## 2. Windows Server — Install & Enable OpenSSH Server

Run in an **elevated PowerShell** (Run as Administrator).

```powershell
# Check whether OpenSSH is available / installed
Get-WindowsCapability -Online | Where-Object Name -like 'OpenSSH*'

# Install the server component
Add-WindowsCapability -Online -Name OpenSSH.Server~~~~0.0.1.0

# Start it now and set it to auto-start on boot
Start-Service sshd
Set-Service -Name sshd -StartupType 'Automatic'

# Confirm it's running and listening
Get-Service sshd
```

### Find the address the collector should target

```powershell
ipconfig      # find the IPv4 the FastAPI host can route to
hostname      # or use a DNS name if you have one
```

### Firewall — allow inbound TCP/22

OpenSSH usually adds the rule automatically, but verify:

```powershell
Get-NetFirewallRule -Name *ssh*

# If the rule is missing, create it:
New-NetFirewallRule -Name sshd `
  -DisplayName 'OpenSSH Server (sshd)' `
  -Enabled True -Direction Inbound -Protocol TCP -Action Allow -LocalPort 22
```

---

## 3. Windows Server — Create the Service Account

Create a **dedicated, non-admin** local user for log collection. Keep it **out of the
Administrators group** — it only needs read access to the log directory.

```powershell
$pw = Read-Host -AsSecureString "Password for svc_logs"
New-LocalUser -Name "svc_logs" -Password $pw -PasswordNeverExpires -AccountNeverExpires
```

### Verify the log path is reachable

```powershell
Test-Path "C:\BEC Logs"

# Match .txt AND rolled files (.txt1, .txt2, …)
Get-ChildItem "C:\BEC Logs\*.txt*" | Select-Object -First 5 Name, Length, LastWriteTime
```

---

## 4. Ubuntu Collector — Generate the SSH Key Pair

On the Ubuntu host, generate a dedicated ed25519 key for the collector.

```bash
ssh-keygen -t ed25519 -f ~/wms1_key -C "fastapi-log-collector"
# Press Enter twice for no passphrase (or set one and supply it to the collector securely)
```

Move the keys to a stable, locked-down location:

```bash
sudo mkdir -p /keys
sudo mv ~/wms1_key      /keys/wms1_key
sudo mv ~/wms1_key.pub  /keys/wms1_key.pub

# Permissions: private key 600, public key 644, owned by the collector user
sudo chmod 600 /keys/wms1_key
sudo chmod 644 /keys/wms1_key.pub
sudo chown amin:amin /keys/wms1_key

# Copy the PUBLIC key line — you'll paste it onto Windows in the next step
cat /keys/wms1_key.pub
```

Example public key line (yours will differ):

```
ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAA...FykD fastapi-log-collector
```

> **Only the `.pub` file ever leaves Ubuntu.** The private key (`/keys/wms1_key`)
> stays on the collector and is referenced by the FastAPI app via `ssh -i`.

---

## 5. Windows Server — Install the Public Key (`authorized_keys`)

Log in as `svc_logs` (or have an admin run this in that user's context) and run in
**PowerShell**:

```powershell
$keyDir = "C:\Users\svc_logs\.ssh"
New-Item -ItemType Directory -Force -Path $keyDir

# Paste the FULL public key line from Ubuntu's /keys/wms1_key.pub
Add-Content "$keyDir\authorized_keys" "ssh-ed25519 AAAAC3Nza...FykD fastapi-log-collector"

# Lock down permissions, or sshd will silently ignore the key
icacls "$keyDir\authorized_keys" /inheritance:r
icacls "$keyDir\authorized_keys" /grant "svc_logs:F" /grant "SYSTEM:F"
```

> ⚠️ **Permissions matter.** Windows OpenSSH refuses to use `authorized_keys` if it is
> writable by other users. The `icacls /inheritance:r` + explicit grants above are required.

> **Note on admin accounts:** For users in the Administrators group, OpenSSH reads
> `C:\ProgramData\ssh\administrators_authorized_keys` instead. `svc_logs` is intentionally
> a standard user, so the per-user `~/.ssh/authorized_keys` is correct here.

---

## 6. Verify Key-Based Login from Ubuntu

From the Ubuntu collector:

```bash
# Should log in WITHOUT a password prompt (passphrase only if you set one on the key)
ssh -i /keys/wms1_key svc_logs@192.168.0.124

# Test SFTP specifically — that's what the collector uses
sftp -i /keys/wms1_key svc_logs@192.168.0.124
# at the sftp> prompt:
sftp> ls "C:/BEC Logs"
```

On first connect you'll be asked to trust the host fingerprint:

```
ED25519 key fingerprint is SHA256:/+TMiWn0Y25I7S+oMoNNBDwZ2KsGf47bbM48GCO33v8.
Are you sure you want to continue connecting (yes/no/[fingerprint])? yes
```

After accepting, the host is added to `~/.ssh/known_hosts` and won't prompt again.

> **Path quirk:** Windows OpenSSH presents paths with forward slashes over SFTP, so use
> `"C:/BEC Logs"` (quoted, because of the space) rather than `C:\BEC Logs`.

---

## 7. Connection Parameters Summary

These are the values the FastAPI collector needs per Windows host:

| Parameter | Value (this host) |
|-----------|-------------------|
| Host / IP | `192.168.0.124` |
| Port | `22` |
| Username | `svc_logs` |
| Private key path | `/keys/wms1_key` |
| Auth method | publickey (ed25519) |
| Log directory | `C:/BEC Logs` |
| File glob | `*.txt*` |

---

## 8. Onboarding a New Windows Host (Checklist)

1. [ ] Install + enable `sshd` on the new host (Section 2)
2. [ ] Open firewall TCP/22 (Section 2)
3. [ ] Create the `svc_logs` service account (Section 3)
4. [ ] Confirm the log path & glob (Section 3)
5. [ ] Generate a **new** key pair on Ubuntu (e.g. `wms2_key`) (Section 4)
6. [ ] Install that host's public key into `svc_logs\.ssh\authorized_keys` (Section 5)
7. [ ] Verify `ssh -i` and `sftp -i` work without a password (Section 6)
8. [ ] Register the host's connection params in the collector config (Section 7)

> Use a **separate key per host** (`wms1_key`, `wms2_key`, …) so a single compromised
> key can be revoked without affecting other hosts.

---

## 9. Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| Still prompted for password after adding key | `authorized_keys` permissions too open | Re-run the `icacls` commands in Section 5 |
| `Permission denied (publickey)` | Wrong key, wrong user, or key not installed | Confirm `cat /keys/wms1_key.pub` matches the line in `authorized_keys` |
| `Connection refused` | sshd not running / firewall blocking | `Get-Service sshd`; verify the firewall rule (Section 2) |
| `Host key verification failed` | known_hosts mismatch (host reinstalled) | Remove the stale line: `ssh-keygen -R 192.168.0.124` |
| SFTP can't find logs | Backslashes / unquoted path with space | Use `ls "C:/BEC Logs"` |
| Admin user key ignored | Account is in Administrators group | Use `administrators_authorized_keys` or a standard user |

---

## 10. Security Notes

- The collector key has **no passphrase** for unattended operation; compensate by:
  - keeping the private key at `chmod 600` owned only by the collector user,
  - using a **non-admin** `svc_logs` account scoped to read the log directory only,
  - using a **distinct key per host** for easy revocation.
- To revoke a host: delete its line from that host's `authorized_keys`, and delete the
  corresponding private key on Ubuntu.
- Never commit private keys (`/keys/wms1_key`) to the repo — only this runbook and the
  public-key handling belong in version control.
