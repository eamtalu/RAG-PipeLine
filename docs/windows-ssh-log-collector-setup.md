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
| `20.254.33.155` (`TMP-AZ-BEC01`) | Windows Server (Azure, domain-joined) | WMS log source (OpenSSH **server**) |

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
# NOTE: the quoted string is only the PROMPT TEXT shown on screen.
# You type the actual password at the prompt (input is hidden).
# Never put the password itself inside this command / this document.
$pw = Read-Host -AsSecureString "TQApniZASQAZKLNTvwJkk2GkNFHGVD"
New-LocalUser -Name "svc_logs" -Password $pw -PasswordNeverExpires -AccountNeverExpires
```

> If you later lose track of the password, reset it with
> `Set-LocalUser -Name svc_logs -Password (Read-Host -AsSecureString "New password")`.
> A failed password login shows up in the sshd debug log as `error: 1326`.

### Log on once as `svc_logs` to create the profile — **required**

`New-LocalUser` creates the account but **not** its profile under `C:\Users`. Until the
account has logged on once, Windows resolves its home directory to `C:\Windows`, so sshd
looks for `C:\Windows\.ssh\authorized_keys` — key auth silently fails and falls back to a
password prompt, even when everything else is correct.

```powershell
runas /user:svc_logs cmd
# Enter the password; a cmd window opens as svc_logs — type `exit` in it.

Test-Path C:\Users\svc_logs    # must print True before you continue
```

> ⚠️ **Do NOT pre-create `C:\Users\svc_logs` by hand.** If a folder with that name already
> exists at first logon, Windows creates the profile at `C:\Users\svc_logs.<HOSTNAME>`
> instead and sshd looks in the wrong place. If you already created it, delete it first
> (you may need `icacls <file> /grant "Administrators:F"` on locked-down files inside).

### Hardened servers: check `AllowGroups` in sshd_config

Managed/domain-joined servers often restrict who may SSH in at all. If this gate blocks
the account, **both** key and password auth fail before they are even attempted.

```powershell
Select-String -Path C:\ProgramData\ssh\sshd_config -Pattern 'AllowGroups|AllowUsers|DenyGroups|DenyUsers'
```

If it returns something like `AllowGroups administrators "openssh users"`, add `svc_logs`
to one of the listed **non-admin** groups (never Administrators — that changes where sshd
looks for the key, see Section 5):

```powershell
Add-LocalGroupMember -Group "OpenSSH Users" -Member svc_logs
Get-LocalGroupMember "OpenSSH Users"    # verify
# No sshd restart needed — group membership is evaluated per connection.
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

**Prerequisite:** `C:\Users\svc_logs` must exist as a *real profile* (the first-logon
step in Section 3), otherwise sshd will not look here at all.

Run in an **elevated PowerShell**:

```powershell
$keyDir = "C:\Users\svc_logs\.ssh"
New-Item -ItemType Directory -Force -Path $keyDir

# Paste the FULL single-line output of `cat /keys/wmsN_key.pub` from Ubuntu between
# the quotes. Use THIS host's key — never reuse a line from another host or from an
# example in a document.
Set-Content C:\Users\svc_logs\.ssh\authorized_keys "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAILEjK6YFLI57ojeEJGxdNrT3AWDBlZoDyHxUcEJ/FykD fastapi-log-collector" -Encoding Asci

# Lock down permissions, or sshd will silently ignore the key
icacls "$keyDir\authorized_keys" /inheritance:r
icacls "$keyDir\authorized_keys" /grant "svc_logs:F" /grant "SYSTEM:F"
```

To confirm the right key landed, compare fingerprints — these two commands must print
the **same** `SHA256:...` value:

```bash
# Ubuntu
ssh-keygen -lf /keys/wms1_key.pub
```

```powershell
# Windows (grant yourself temporary read access first if denied)
ssh-keygen -lf C:\Users\svc_logs\.ssh\authorized_keys
```

> ⚠️ **Permissions matter.** Windows OpenSSH refuses to use `authorized_keys` if it is
> writable by other users. The `icacls /inheritance:r` + explicit grants above are required.
> Note these ACLs also lock **Administrators** out — to read/edit/delete the file later,
> first run `icacls "$keyDir\authorized_keys" /grant "Administrators:F"`.

> **Encoding matters too.** Use `Set-Content -Encoding Ascii` (or `Add-Content`).
> `Out-File` / redirection writes UTF-16, which sshd cannot parse.

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

| Parameter | `LAPTOP-DPJEJEU6` | `TMP-AZ-BEC01` |
|-----------|-------------------|----------------|
| Host / IP | `192.168.0.124` | `20.254.33.155` |
| Port | `22` | `22` |
| Username | `svc_logs` | `svc_logs` |
| Private key path | `/keys/wms1_key` | `/keys/wms1_key` |
| Auth method | publickey (ed25519) | publickey (ed25519) |
| Log directory | `C:/BEC Logs` | `C:/BEC Logs` |
| File glob | `*.txt*` | `*.txt*` |

---

## 8. Onboarding a New Windows Host (Checklist)

1. [ ] Install + enable `sshd` on the new host (Section 2)
2. [ ] Open firewall TCP/22 (Section 2)
3. [ ] Create the `svc_logs` service account (Section 3)
4. [ ] **Log on once as `svc_logs`** so the `C:\Users\svc_logs` profile exists (Section 3)
5. [ ] Check `sshd_config` for `AllowGroups` / `AllowUsers`; add `svc_logs` to an allowed group if needed (Section 3)
6. [ ] Confirm the log path & glob (Section 3)
7. [ ] Generate a **new** key pair on Ubuntu (e.g. `wms2_key`) (Section 4)
8. [ ] Install **that host's** public key into `svc_logs\.ssh\authorized_keys`; verify fingerprints match (Section 5)
9. [ ] Verify `ssh -i` and `sftp -i` work without a password (Section 6)
10. [ ] Register the host's connection params in the collector config (Section 7)

> Use a **separate key per host** (`wms1_key`, `wms2_key`, …) so a single compromised
> key can be revoked without affecting other hosts.

---

## 9. Troubleshooting

A password prompt after installing the key means Windows rejected the key **silently**
and fell back to password auth. Don't guess — get the reason from the logs (see
"Reading the sshd logs" below). Confirmed causes we have hit, roughly in the order to
check them:

| Symptom / log message | Likely cause | Fix |
|---------|-------------|-----|
| Debug log: `trying public key file C:\Windows\.ssh/authorized_keys` | **`svc_logs` never logged on** — profile doesn't exist, home dir resolves to `C:\Windows` | First-logon step in Section 3 (`runas /user:svc_logs cmd`). Delete any hand-made `C:\Users\svc_logs` folder first |
| Event log: `User svc_logs ... not allowed because none of user's groups are listed in AllowGroups` | Hardened `sshd_config` gates SSH logins by group | `Add-LocalGroupMember -Group "OpenSSH Users" -Member svc_logs` (Section 3) |
| Key offered (`ssh -vvv` shows `Offering public key`) but rejected | Key in `authorized_keys` isn't this host's key (e.g. copy-pasted from another host/doc) | Compare `ssh-keygen -lf` fingerprints on both sides (Section 5) |
| Debug log: `bad ownership or modes` | `authorized_keys` permissions too open | Re-run the `icacls` commands in Section 5 |
| Debug log: `Windows authentication failed ... error: 1326` on password attempt | Wrong password (e.g. the `Read-Host` prompt-vs-password confusion) | `Set-LocalUser -Name svc_logs -Password (Read-Host -AsSecureString "New password")` |
| `Permission denied (publickey)` | Wrong key, wrong user, or key not installed | Confirm `cat /keys/wms1_key.pub` matches the line in `authorized_keys` |
| `Connection refused` | sshd not running / firewall blocking | `Get-Service sshd`; verify the firewall rule (Section 2) |
| `Host key verification failed` | known_hosts mismatch (host reinstalled) | Remove the stale line: `ssh-keygen -R <host-ip>` |
| SFTP can't find logs | Backslashes / unquoted path with space | Use `ls "C:/BEC Logs"` |
| Admin user key ignored | Account is in Administrators group | Use `administrators_authorized_keys` or a standard user |
| Admin can't read/edit/delete `authorized_keys` (`Access denied`) | Section 5 ACLs lock out Administrators too — this is expected | `icacls <file> /grant "Administrators:F"`, do the change, re-run the Section 5 `icacls` lockdown |

### Diagnosing from the Ubuntu side

```bash
ssh -vvv -i /keys/wms1_key svc_logs@<host> 2>&1 | grep -iE 'offering|identity|denied|sign'
```

If you see `Offering public key: /keys/wms1_key ...` followed by a password prompt, the
client side is fine — the rejection reason is on the Windows side.

### Reading the sshd logs on Windows

Quick look without any config change (default INFO level — shows `AllowGroups` blocks
and password failures, but **not** why a key was refused):

```powershell
Get-WinEvent -LogName 'OpenSSH/Operational' -MaxEvents 30 | Format-List TimeCreated, Message
```

For the full story (exact `authorized_keys` path tried + refusal reason), enable debug
file logging. `C:\ProgramData` is a **hidden** folder — it won't show in Explorer, but
every command below works regardless:

```powershell
notepad C:\ProgramData\ssh\sshd_config
# change:  #SyslogFacility AUTH   ->  SyslogFacility LOCAL0
#          #LogLevel INFO         ->  LogLevel DEBUG3
Restart-Service sshd

# Reproduce the failed login from Ubuntu once, then:
Select-String -Path C:\ProgramData\ssh\logs\sshd.log -Pattern 'svc_logs|authorized|denied|refused|bad|trying' | Select-Object -Last 40
```

> Revert to `LogLevel INFO` and `Restart-Service sshd` when done — DEBUG3 is very verbose.

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
