#!/bin/bash
set -e
pytest --cov=src/py_load_pubmedcentral tests/test_config.py
pytest --cov=src/py_load_pubmedcentral --cov-append tests/test_parser.py
pytest --cov=src/py_load_pubmedcentral --cov-append tests/test_db.py
pytest --cov=src/py_load_pubmedcentral --cov-append tests/test_acquisition.py
pytest --cov=src/py_load_pubmedcentral --cov-append tests/test_cli.py
pytest --cov=src/py_load_pubmedcentral --cov-append tests/test_delta_load.py
pytest --cov=src/py_load_pubmedcentral --cov-append tests/test_end_to_end.py
pytest --cov=src/py_load_pubmedcentral --cov-append tests/test_full_load_optimization.py
pytest --cov=src/py_load_pubmedcentral --cov-append tests/test_full_pipeline.py
pytest --cov=src/py_load_pubmedcentral --cov-append tests/test_pipeline_integration.py
pytest --cov=src/py_load_pubmedcentral --cov-append tests/test_retractions.py
pytest --cov=src/py_load_pubmedcentral --cov-append tests/test_s3_acquisition.py

coverage report
