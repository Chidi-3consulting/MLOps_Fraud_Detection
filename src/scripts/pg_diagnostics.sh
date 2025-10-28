#!/usr/bin/env bash
# Collect helpful Postgres diagnostics to troubleshoot authentication failures.
# Run this ON THE POSTGRES HOST (not in a random container) with a user that can read Postgres logs
# or run as root. The script will try several common locations and print concise hints.

set -euo pipefail

PGUSER="${PGUSER:-postgres}"
PGPASSWORD="${PGPASSWORD:-}"

echo "Postgres diagnostics - $(date -u)"
echo "Running as user: $(whoami)"
echo

if command -v psql >/dev/null 2>&1; then
  echo "psql available: $(psql --version)"
else
  echo "psql not found in PATH. If you have psql, add it to PATH or run this script as a postgres system user."
fi

echo
echo "--- Postgres configuration (via psql if available) ---"
if command -v psql >/dev/null 2>&1; then
  export PGPASSWORD="$PGPASSWORD"
  echo "config_file:" $(psql -U "$PGUSER" -At -c "SHOW config_file;" 2>/dev/null || echo "(psql access denied)")
  echo "hba_file:" $(psql -U "$PGUSER" -At -c "SHOW hba_file;" 2>/dev/null || echo "(psql access denied)")
  echo "listen_addresses:" $(psql -U "$PGUSER" -At -c "SHOW listen_addresses;" 2>/dev/null || echo "(psql access denied)")
else
  echo "Skipping psql-backed config queries"
fi

echo
echo "--- Searching for authentication failures in common log locations ---"

LOG_PATTERNS=("password authentication failed" "authentication failed" "no pg_hba.conf entry")

# Check system journal (systemd)
if command -v journalctl >/dev/null 2>&1; then
  echo "journalctl (postgresql) last 200 lines with auth-related messages:"
  journalctl -u postgresql -n 200 --no-pager 2>/dev/null | grep -i -E "${LOG_PATTERNS[0]}|${LOG_PATTERNS[1]}|${LOG_PATTERNS[2]}" || true
fi

# Common log directories
COMMON_LOG_DIRS=("/var/log/postgresql" "/var/lib/postgresql/data/pg_log" "/var/lib/pgsql/data/pg_log" "/var/lib/postgres/data/pg_log")
found=0
for d in "${COMMON_LOG_DIRS[@]}"; do
  if [ -d "$d" ]; then
    echo
    echo "Searching logs in: $d"
    found=1
    grep -i -R --line-number -E "${LOG_PATTERNS[0]}|${LOG_PATTERNS[1]}|${LOG_PATTERNS[2]}" "$d" 2>/dev/null | tail -n 200 || true
  fi
done

if [ $found -eq 0 ]; then
  echo "No common log directories found. If Postgres runs in Docker, run: docker logs <postgres_container> --tail 200"
fi

echo
echo "--- If available, show last 200 lines of the pg_hba.conf (for inspection) ---"
HBA_FILE=""
if command -v psql >/dev/null 2>&1; then
  HBA_FILE=$(psql -U "$PGUSER" -At -c "SHOW hba_file;" 2>/dev/null || true)
fi
if [ -n "$HBA_FILE" ] && [ -f "$HBA_FILE" ]; then
  echo "pg_hba.conf: $HBA_FILE"
  echo "--- start pg_hba.conf (tail 200) ---"
  tail -n 200 "$HBA_FILE"
  echo "--- end pg_hba.conf ---"
else
  echo "pg_hba.conf file not found via psql. If you know the file path, run: sudo tail -n 200 /path/to/pg_hba.conf"
fi

echo
echo "--- Quick psql active connections (if psql available) ---"
if command -v psql >/dev/null 2>&1; then
  echo "Recent backends (pg_stat_activity):"
  psql -U "$PGUSER" -At -c "SELECT pid, usename, client_addr, client_port, state, backend_start FROM pg_stat_activity ORDER BY backend_start DESC LIMIT 20;" 2>/dev/null || echo "(psql access denied)"
fi

echo
echo "Diagnostics finished. Paste any 'FATAL' or 'password authentication failed' lines here and I'll interpret them."
