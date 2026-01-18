# Docker Production Setup.

> [!NOTE]
> This will get you an environment that runs on 0.0.0.0:5000 and scrapes sheepit every day at 7PM.

## Requirements

- Docker

## Instructions

- Clone this repo.
- Create a `docker-compose.yml` and `.env` file. There are example templates you can use for both.
- Run `docker compose up -d --build`

# Local Production Setup

> [!WARNING]
> Setting this up locally is not recommended for production, it is highly recommended to use docker.

## Requirements

- Rust
- Python

## Instructions

- Clone this repo.
- Run `./scripts/setup.sh` to do initial setup.
- To get the webserver running, run `./scripts/run.sh` This will expose a server on 0.0.0.0:5000.
- Create an `.env` file to configure the app. You can use the provided `.env.example` file as a reference.
- To scrape team data you can run `./ibu_dashboard/sheepit_scraper.py`, or use a cron job.
