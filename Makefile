.PHONY: test lint bundle-validate bundle-deploy bundle-run clean

PYTHON := python3

test:
	PYTHONPATH=src $(PYTHON) -m unittest discover -s tests -v

syntax-check:
	$(PYTHON) -m py_compile src/uber_platform/*.py notebooks/*.py pipelines/*.py tests/*.py

bundle-validate:
	databricks bundle validate

bundle-deploy:
	databricks bundle deploy -t dev

bundle-run:
	databricks bundle run uber_end_to_end_lakehouse_job -t dev

clean:
	rm -rf __pycache__ src/uber_platform/__pycache__ tests/__pycache__ build dist *.egg-info tmp_test_output data_cache
