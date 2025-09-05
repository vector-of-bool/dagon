.SILENT:

lint: format-check
	pyright src/

format:
	ruff format src/

format-check:
	ruff format src/ --check --quiet

test:
	pytest src/

build: lint format-check
	uv build

INDEX := test-pypi
publish: build
	uv publish --index="$(INDEX)"
