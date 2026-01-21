#!/usr/bin/env bash

# This script uses tmux for live reloading css and the site.

SESSION_NAME="dev"

CMD1="./.venv/bin/python main.py"
CMD2="npx @tailwindcss/cli -i ./static/input.css -o ./static/tailwind.css --watch"

tmux new-session -d -s "$SESSION_NAME" -n "dashboard" sh -lc "$CMD1"
tmux new-window  -t "$SESSION_NAME:1" -n "tailwind" sh -lc "$CMD2"
tmux new-window -t "$SESSION_NAME:2" -n "cmd3" "$SHELL"


tmux attach -t "$SESSION_NAME"
