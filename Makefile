PYTHON ?= python3
PYTHON_SOURCE_DIRS = aiven_db_migrate/ test/
PG_VERSIONS = 10 11 12 13 14
VERSION = $(shell hatch version)
RELEASE = 1

generated = aiven_db_migrate/migrate/version.py


all: $(generated)

aiven_db_migrate/migrate/version.py:
	echo "__version__ = \"$(VERSION)\"" > $@


build-dep-fedora:
	sudo dnf -y install --best --allowerasing \
		$(foreach ver,$(PG_VERSIONS),postgresql$(ver)-server) \
		python3-devel \
		python3-wheel \
		python3-flake8 \
		python3-isort \
		python3-mypy \
		python3-psycopg2 \
		python3-pylint \
		python3-pytest \
		python3-yapf \
		python3-hatch-vcs \
		hatch \
		rpmdevtools \
		tar

flake8: $(generated)
	$(PYTHON) -m flake8 $(PYTHON_SOURCE_DIRS)

pylint: $(generated)
	$(PYTHON) -m pylint --rcfile .pylintrc $(PYTHON_SOURCE_DIRS)

mypy: $(generated)
	$(PYTHON) -m mypy $(PYTHON_SOURCE_DIRS)

isort: $(generated)
	$(PYTHON) -m isort $(PYTHON_SOURCE_DIRS)

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

test: $(generated)
	$(PYTHON) -m pytest -v -r test

clean:
	$(RM) $(generated)
	$(RM) aiven_db_migrate-*.tar.gz
	$(RM) -r dist

rpm:
	hatch build
	rpmbuild -bb aiven-db-migrate.spec \
		--define '_sourcedir $(CURDIR)/dist' \
		--define '_topdir $(CURDIR)/dist/rpmbuild' \
		--define '_rpmdir $(CURDIR)/dist/rpms' \
		--define 'source_dist $(PWD)/dist/aiven_db_migrate-$(VERSION).tar.gz' \
		--define 'major_version $(VERSION)' \
		--define 'release_number $(RELEASE)'

.PHONY: build-dep-fedora test clean flake8 pylint mypy isort yapf static-checks validate-style rpm
