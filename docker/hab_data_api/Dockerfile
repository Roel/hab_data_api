FROM python:3.12-slim AS base

# Prevents Python from writing pyc files
ENV PYTHONDONTWRITEBYTECODE=1

# Keeps Python from buffering stdout and stderr to avoid crashes without any logs
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Create a non-privileged user
ARG UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/appuser" \
    --shell "/sbin/nologin" \
    --uid "${UID}" \
    appuser

# Update pip
RUN pip install --upgrade pip

# Install linux dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Download dependencies as a separate step to take advantage of Docker's caching.
# Leverage a cache mount to /root/.cache/pip to speed up subsequent builds.
# Leverage a bind mount to requirements.txt to avoid having to copy them into this layer.
RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=bind,source=requirements.txt,target=requirements.txt \
    python -m pip install -r requirements.txt

# Copy the source code into the container.
COPY . .

# Expose the port that the application listens on.
EXPOSE 8080

# Switch to the non-privileged user to run the application.
USER appuser

HEALTHCHECK CMD curl --fail http://localhost:8080/status/health || exit 1

# Run server
CMD hypercorn --bind 0.0.0.0:8080 hab_data_api/main.py:app