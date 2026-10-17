# Security policy

SonicNet changes how your PC routes traffic (System mode runs an elevated helper) and handles
browser cookies for signed-in downloads, so security reports are taken seriously.

## Reporting a vulnerability

**Please don't open a public issue.** Use GitHub's private reporting instead:
[Report a vulnerability](https://github.com/nettanvirdev/sonicnet/security/advisories/new).

Include what you found, how to reproduce it, and what an attacker could do with it. You'll get an
acknowledgement within a few days, and a fix or a plan once it's understood. Credit is given in
the release notes unless you'd rather not be named.

## Scope

Especially interesting:
- the System mode helper (`sonicnet-system.exe`), which runs elevated, and its pipe protocol
- the browser bridge on `127.0.0.1` (ports 19320–19322) and the extension
- anything that could make a download write outside its folder, or corrupt a file silently
- cookie handling and storage (DPAPI-sealed in `%APPDATA%\SonicNet`)

## Supported versions

Only the latest release gets security fixes.
