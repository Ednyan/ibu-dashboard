#!/usr/bin/env bash

# Update repo
git pull origin main

# Update python modules

# Python setup
if [[ "$1" == "uv" ]]; then
    uv pip install -r requirements.txt --upgrade --python .venv/bin/python
else
    ./.venv/bin/python -m pip install --upgrade -r requirements.txt
fi
