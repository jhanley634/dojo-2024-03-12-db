
all: data/title.basics.tsv.gz

%.tsv.gz:
	$(MAKE) -C data/

SHELL := bash
PROJECT := $(shell basename $(CURDIR))
# Typically BIN will be $(HOME)/miniconda3/bin
BIN := $(shell dirname $(CONDA_EXE))
ACTIVATE = source $(BIN)/activate $(PROJECT)

lint:
	$(ACTIVATE) && black . && isort . && ruff check
	$(ACTIVATE) && mypy --no-namespace-packages --ignore-missing-imports .

test:
	$(ACTIVATE) && python --version
