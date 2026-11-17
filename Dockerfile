# The base image is pinned by digest for reproducible builds. To upgrade,
# resolve the current digest of the wanted tag and update BOTH below:
#   docker buildx imagetools inspect python:3.12-alpine
# Tag 3.12-alpine resolved on 2026-11-17.
FROM python:3.12-alpine@sha256:c5ba3deef00c7758561a0f03c26f44f10589263d40e732b8748e0d1f38f6dd89

# Emit logs immediately so `docker logs` shows them in real time
ENV PYTHONUNBUFFERED=1

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Run as an unprivileged user. The app needs write access to the working
# directory to create the log directory at runtime.
RUN addgroup -S app && adduser -S appuser -G app \
    && chown -R appuser:app /usr/src/app
USER appuser

CMD [ "python", "main.py" ]
