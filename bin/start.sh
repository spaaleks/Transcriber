#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
# Go to project directory
cd "${SCRIPT_DIR}/.." || exit 1

set -e
python3 -m pip install -r requirements.txt
exec python3 app.py "$@"
