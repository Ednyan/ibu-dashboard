FROM debian:trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates openssl libssl3 curl cron rustup build-essential python3 && rm -rf /var/lib/apt/lists/*
RUN rustup default stable
RUN curl -Ls https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:/root/.cargo/bin:${PATH}"

WORKDIR /ibu
COPY . .
RUN ./scripts/setup.sh uv
RUN cargo clean
RUN echo "0 19 * * * root cd /ibu && mkdir -p /ibu/logs && /ibu/.venv/bin/python /ibu/ibu_dashboard/sheepit_scraper.py >> /ibu/logs/cron.log 2>&1" > /etc/cron.d/ibu && chmod 0644 /etc/cron.d/ibu

EXPOSE 5000

ENTRYPOINT ["./scripts/docker-entrypoint.sh"]