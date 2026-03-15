.PHONY: generate clean install test build publish docs fmt check update-deps

generate:
	openapi-python-client generate --path ../openapi.yaml --output-path src/antfly --overwrite --config .openapi-generator-config.yaml
	uv run ruff check src/antfly/client_generated --fix
	uv run ruff format src/antfly/client_generated

clean:
	rm -rf build dist *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete

install:
	uv sync

test:
	uv run pytest tests --cov=antfly --cov-report=xml

build: clean generate
	uv build

publish: build
	uv publish

docs:
	cd docs && make html

check:
	uv run ruff check src tests --fix
	uv run ruff format src tests --fix
	uv run pyright src tests

update-deps:
	uv lock --upgrade && uv sync
