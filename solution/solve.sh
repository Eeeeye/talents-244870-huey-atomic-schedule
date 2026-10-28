#!/bin/bash
set -euo pipefail
cd /app/huey
patch --batch --forward -p1 < /solution/atomic-promotion.patch
