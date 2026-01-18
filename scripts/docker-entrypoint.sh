#!/usr/bin/env bash

cron
./.venv/bin/uwsgi --http 0.0.0.0:5000 --master --enable-threads --lazy-apps --processes 1 -w main:app