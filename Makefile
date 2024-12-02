.PHONY: test test-cov test-watch

VENV_PATH := /Users/eddyt/Algo/projects/df_checker/.venv
PYTEST := $(VENV_PATH)/bin/pytest

test:
	$(PYTEST) -v

test-cov:
	$(PYTEST) --cov=src --cov-report=term-missing --cov-report=html

test-watch:
	$(PYTEST) -f -v
