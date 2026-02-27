FROM python:3.12-slim

WORKDIR /app

# Install uv for package management
RUN pip install --no-cache-dir uv

# Copy only dependency files first for layer caching
COPY pyproject.toml uv.lock ./

# Install dependencies only (skip the project itself)
RUN uv sync --frozen --no-install-project --no-dev

# Copy the source code
COPY src/ ./src/

# Install the project package into the existing venv
RUN uv pip install --no-cache-dir -e .

# Create directory for SQLite database
RUN mkdir -p /data

# Use the venv's python for the entrypoint
ENV PATH="/app/.venv/bin:$PATH"
ENTRYPOINT ["python", "-m", "eventagent"]
