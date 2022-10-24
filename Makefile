.PHONY: docker-build

# Sets a default image to be built/run
# Override this by running: make <target> IMAGE=foo
PACKAGE_NAME := dagster_jaffle_shop
IMAGE_NAME := dagster-jaffle-shop

clean:
	find $(PACKAGE_NAME) -type d -name '__pycache__' -exec rm -rf {} \;

dagit:
	dagit --package-name $(PACKAGE_NAME)

materialize:
	dagster job execute --package-name $(PACKAGE_NAME) -j materialize_all_assets_job

evaluate:
	dagster job execute --package-name $(PACKAGE_NAME) -j evaluate_duckdb_tables_job

format:
	black .

setup:
	touch /tmp/dagster.yaml

test:
	pytest tests

docker-build:
	docker build -t $(IMAGE_NAME) .

docker-dagit:
	docker run $(IMAGE_NAME) dagit --package-name $(PACKAGE_NAME) -p 3000:3000 -h 0.0.0.0
