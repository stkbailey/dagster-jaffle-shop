clean:
	find dagster_jaffle_shop -type d -name '__pycache__' -exec rm -rf {} \;

dagit:
	dagit --package-name dagster_jaffle_shop

materialize:
	dagster job execute --package-name dagster_jaffle_shop -j materialize_all_assets_job

format:
	black dagster_jaffle_shop tests

setup:
	touch /tmp/dagster.yaml

test:
	pytest tests
