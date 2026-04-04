# Docker

These will get you an environment that runs on `http://0.0.0.0:5000` and scrapes sheepit every day at 7PM.

> [!WARNING]
> Running this in production not really recommended as it gives little to no benefit over using the public instance. This is mainly intended as a reference point.

## Using the Provided Docker Images

### Requirements

- Docker
- Docker Compose

### Instructions

Create `docker-compose.yml`, e.g.

```yml
services:
  ibu:
    image: ghcr.io/ednyan/ibu-dashboard:latest
    ports:
      - "5000:5000"
    container_name: ibu-dashboard
    restart: unless-stopped

    env_file:
      - .env

    volumes:
      - ./docker/data/scraped_team_info:/ibu/Scraped_Team_Info
      - ./docker/data/scraped_teams_points:/ibu/Scraped_Teams_Points
      - ./docker/config:/ibu/config
      - ./docker/logs/:/ibu/logs
      - ./docker/notification_history/:/ibu/notification_history/
      - /etc/localtime:/etc/localtime:ro
```

- Copy the [env.example](../.env.example) file to `.env` and edit as needed.
- Run `docker compose up -d --build`.

## Local Docker Build setup

### Requirements

- Docker
- Docker Compose

### Instructions

- Clone this repo.
- Create `docker-compose.override.yml`, e.g.

```yml
services:
  ibu:
    ports:
      - "5000:5000"
```

- Copy the [env.example](../.env.example) file to `.env` and edit as needed.
- Run `docker compose up -d --build`.
