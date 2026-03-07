#!/usr/bin/env bash

./.venv/bin/uwsgi --http 0.0.0.0:5000 --master --enable-threads --lazy-apps -w main:app
