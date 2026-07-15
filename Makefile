PYTHON ?= python3

.PHONY: install check test

install:
	$(PYTHON) -m pip install -e .

check:
	$(PYTHON) -m compileall -q src tests

test:
	PYTHONPATH=src $(PYTHON) -m unittest discover -s tests/unit -v
