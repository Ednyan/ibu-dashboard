# Docker Production Setup.

This will get you an environment that runs on `http://0.0.0.0:5000` and scrapes sheepit every day at 7PM.

> [!WARNING]
> Running this in production not really recommended as it gives little to no benifit over using the public instance. This is mainly intended as a reference point.

## Requirements

- Docker
- Docker Compose

## Instructions

- Clone this repo.
- Create a `docker-compose.override.yml`, e.g.

```yml
services:
  ibu:
    ports:
      - "5000:5000"
```

- Copy the `env.example` file to `.env` and edit as needed.
- Run `docker compose up -d --build`.
