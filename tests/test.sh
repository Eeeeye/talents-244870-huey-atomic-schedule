#!/bin/bash
# Binary reward. Initialize before setup so failures cannot reuse an old score.
set -u
mkdir -p /logs/verifier || exit 1
printf '0\n' > /logs/verifier/reward.txt || exit 1
export PYTHONDONTWRITEBYTECODE=1
export PYTHONUNBUFFERED=1
export TASK_SOURCE=/app/huey
cd /app/huey || exit 0
if timeout --signal=TERM --kill-after=5s 150s python3 -I -B /tests/test_promotion.py > /logs/verifier/tests.log 2>&1; then
    printf '1\n' > /logs/verifier/reward.txt
fi
cat /logs/verifier/tests.log
exit 0
