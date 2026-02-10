.SILENT:

lint: format-check
	pyright src/

format:
	ruff format src/

format-check:
	ruff format src/ --check --quiet

test:
	pytest src/

typecheck:
	pyright

build: lint format-check
	uv build

SPHINX_ARGS ?= -j auto --builder=dirhtml --write-all

.PHONY: docs docs-serve
docs:
	sphinx-build $(SPHINX_ARGS) docs/ build/docs/

docs-serve:
	sphinx-autobuild $(SPHINX_ARGS) docs/ build/docs/ --watch src/

INDEX := test-pypi
publish: build
	uv publish --index="$(INDEX)"

many-test: \
	test-python-version(3.12) test-python-version(3.13) \
	test-python-version(3.14)

test-python-version(%):
	echo "Testing Python $(%F)"
	uv run --isolated --python "$(%F)" $(MAKE) test
