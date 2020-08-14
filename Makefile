
PYTHON ?= python3
PYTHON_SOURCE_DIRS = aiven_db_migrate/ test/
PG_VERSIONS = 95 96 10 11 12

generated = aiven_db_migrate/migrate/version.py


all: $(generated)

aiven_db_migrate/migrate/version.py:
	echo "__version__ = \"$(shell git describe)\"" > $@

build-dep-fedora:
	sudo dnf -y install --best --allowerasing \
		$(foreach ver,$(PG_VERSIONS),postgresql$(ver)-server) \
		python3-flake8 \
		python3-isort \
		python3-mypy \
		python3-psycopg2 \
		python3-pylint \
		python3-pytest \
		python3-yapf \
		rpm-build

flake8: $(generated)
	$(PYTHON) -m flake8 $(PYTHON_SOURCE_DIRS)

pylint: $(generated)
	$(PYTHON) -m pylint --rcfile .pylintrc $(PYTHON_SOURCE_DIRS)

mypy: $(generated)
	$(PYTHON) -m mypy $(PYTHON_SOURCE_DIRS)

isort: $(generated)
	$(PYTHON) -m isort --recursive $(PYTHON_SOURCE_DIRS)

yapf: $(generated)
	$(PYTHON) -m yapf --parallel --recursive --in-place $(PYTHON_SOURCE_DIRS)

static-checks: flake8 pylint mypy

validate-style:
	$(eval CHANGES_BEFORE := $(shell mktemp))
	git diff > $(CHANGES_BEFORE)
	$(MAKE) isort yapf
	$(eval CHANGES_AFTER := $(shell mktemp))
	git diff > $(CHANGES_AFTER)
	diff $(CHANGES_BEFORE) $(CHANGES_AFTER)
	-rm $(CHANGES_BEFORE) $(CHANGES_AFTER)

lint: validate-style static-checks

.PHONY: test
test: $(generated)
	$(PYTHON) -m pytest -v -r test

clean:
	$(RM) aiven_db_migrate/migrate/version.py
