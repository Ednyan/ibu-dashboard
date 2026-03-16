FROM debian:trixie-slim
WORKDIR /ibu
COPY . .

ENV PATH="/root/.local/bin:/root/.cargo/bin:${PATH}"
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl npm python3-minimal cron \
 && curl -Ls https://astral.sh/uv/install.sh | sh \
 && ./scripts/setup.sh uv \
 && rm -rf ~/.cache/uv ./node_modules ~/.local/bin/uv ~/.cargo \
 && apt-get purge -y curl npm \
 && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/*

# Setup cron stuff
RUN echo "0 19 * * * root cd /ibu && mkdir -p /ibu/logs && /ibu/.venv/bin/python /ibu/ibu_dashboard/sheepit_scraper.py >> /ibu/logs/cron.log 2>&1" > /etc/cron.d/ibu && chmod 0644 /etc/cron.d/ibu

EXPOSE 5000

ENTRYPOINT ["./scripts/docker-entrypoint.sh"]
