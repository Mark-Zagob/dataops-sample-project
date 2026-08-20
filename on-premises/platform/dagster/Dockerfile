# =================================================================
# PRODUCTION-GRADE DOCKERFILE
# =================================================================
# Image dùng chung cho:
# - dagster-platform: Dagster Webserver
# - dagster-daemon: Dagster Daemon
#
# Healthcheck được cấu hình trong docker-compose.yml,
# vì mỗi service có tiêu chí health khác nhau.
# =================================================================

FROM python:3.10-slim

WORKDIR /opt/dagster

# -----------------------------------------------------------------
# Cài đặt system dependencies cần thiết cho psycopg2
# -----------------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# -----------------------------------------------------------------
# Copy requirements.txt trước để tận dụng Docker layer caching
# -----------------------------------------------------------------
COPY ./platform/dagster/requirements.txt ./requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

# -----------------------------------------------------------------
# Copy Dagster configuration files
# -----------------------------------------------------------------
COPY ./platform/dagster/dagster.yaml ./dagster_home/dagster.yaml
COPY ./platform/dagster/workspace.yaml ./dagster_home/workspace.yaml

# -----------------------------------------------------------------
# Copy healthcheck scripts
# -----------------------------------------------------------------
COPY ./platform/dagster/healthchecks/ ./healthchecks/

# -----------------------------------------------------------------
# Copy User Code
# Production Mode: COPY vào image, không bind mount
# -----------------------------------------------------------------
COPY ./user_code/ ./user_code/

# -----------------------------------------------------------------
# Tạo DAGSTER_HOME
# -----------------------------------------------------------------
RUN mkdir -p /opt/dagster/dagster_home

ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV PYTHONUNBUFFERED=1

EXPOSE 3000

# -----------------------------------------------------------------
# Default command là webserver.
# Service dagster-daemon sẽ override command trong docker-compose.yml.
# -----------------------------------------------------------------
CMD [ \
    "dagster-webserver", \
    "-h", "0.0.0.0", \
    "-p", "3000", \
    "-w", "/opt/dagster/dagster_home/workspace.yaml" \
]