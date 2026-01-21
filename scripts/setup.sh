#!/usr/bin/env bash

set -e

# Python setup
if [[ "$1" == "uv" ]]; then
    echo "Creating venv"
    uv venv .venv
    echo "Installing deps"
    uv pip install -r requirements.txt --python .venv/bin/python
else
    echo "Creating venv"
    python3 -m venv .venv
    echo "Installing deps"
    ./.venv/bin/python -m pip install -r requirements.txt
fi


# Rust setup
./scripts/build.sh

# Node setup
npm install 
npx @tailwindcss/cli -i ./static/input.css -o ./static/tailwind.css --minify

