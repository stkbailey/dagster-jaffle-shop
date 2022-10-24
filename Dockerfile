FROM python:3.10-slim-buster

# Update system dependencies
RUN apt-get update && apt-get install -y build-essential

# Add a dagster.yaml in the working directory
RUN touch /tmp/dagster.yaml

# copy requirements to cache in docker layer
COPY pyproject.toml poetry.lock ./

# Install dependencies via Poetry, for extra stability
RUN pip install poetry==1.2.0
RUN poetry config virtualenvs.create false \
    && poetry install --no-dev

# copy all other system files over (ignoring those in .dockerignore)
COPY . .

CMD ["dagit", "-h", "0.0.0.0", "-p","3000", "-w", "./workspace.yaml"]