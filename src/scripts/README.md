Postgres diagnostics
====================

These helper scripts gather information from a Postgres host to troubleshoot authentication failures.

Files:
- `pg_diagnostics.sh` — Bash script for Linux/systemd/docker hosts. Run as root or a user that can read Postgres logs.
- `pg_diagnostics.ps1` — PowerShell script for Windows hosts.

How to use (Linux):

1. Copy the script to the Postgres host (or edit it there).
2. Run it as root or a postgres-capable user:

   sudo bash ./pg_diagnostics.sh

3. Paste any lines that include `password authentication failed` or `FATAL` here and I'll interpret them and recommend the exact `pg_hba.conf` or password fix.

How to use (Docker):

If Postgres runs in Docker, run on the Docker host:

  docker logs <postgres_container_name_or_id> --tail 200 | grep -i "password authentication failed\|authentication failed\|no pg_hba.conf entry"

Security note: do not commit production passwords into the repository. Use `.env.example` and local `.env` files kept out of version control.
