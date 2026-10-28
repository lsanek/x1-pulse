#!/bin/bash
# X1 Pulse: refresh data every 15 minutes. Safe to start more than once (lock).
# Auto-update: takes newer pulse.py / index.html from the exchange folder
# (only after a successful syntax check).
X="/mnt/c/Users/libor/Documents/x1-pulse-exchange"
PY="$HOME/x1-rugcheck-bot/.venv/bin/python"
cd "$HOME/x1-pulse" || exit 1
exec 9>/tmp/x1pulse.lock
flock -n 9 || exit 0
while true; do
  [ -f pulse.log ] && [ "$(stat -c%s pulse.log)" -gt 2000000 ] && mv pulse.log pulse.log.old
  if [ -f "$X/pulse.py" ] && [ "$X/pulse.py" -nt pulse.py ]; then
    tr -d '\r' < "$X/pulse.py" > pulse.py.new
    if "$PY" -m py_compile pulse.py.new 2>>pulse.log; then
      mv pulse.py.new pulse.py; echo "[$(date '+%F %T')] auto-update: pulse.py" >> pulse.log
    else
      rm -f pulse.py.new; echo "[$(date '+%F %T')] auto-update REJECTED: pulse.py syntax error" >> pulse.log
    fi
  fi
  if [ -f "$X/index.html" ] && [ "$X/index.html" -nt index.html ]; then
    cp "$X/index.html" index.html && echo "[$(date '+%F %T')] auto-update: index.html" >> pulse.log
  fi
  timeout 600 "$PY" pulse.py >> pulse.log 2>&1 9>&-
  tail -n 40 pulse.log > "$X/pulse_status.txt" 2>/dev/null
  sleep 900 9>&-
done
