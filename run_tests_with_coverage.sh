#!/bin/bash
set -e
python3 -m pytest --cov=src/py_load_pubmedcentral tests/test_config.py
python3 -m pytest --cov=src/py_load_pubmedcentral --cov-append tests/test_parser.py
python3 -m pytest --cov=src/py_load_pubmedcentral --cov-append tests/test_db.py
python3 -m pytest --cov=src/py_load_pubmedcentral --cov-append tests/test_acquisition.py
python3 -m pytest --cov=src/py_load_pubmedcentral --cov-append tests/test_cli.py
python3 -m pytest --cov=src/py_load_pubmedcentral --cov-append tests/test_delta_load.py
python3 -m pytest --cov=src/py_load_pubmedcentral --cov-append tests/test_end_to_end.py
python3 -m pytest --cov=src/py_load_pubmedcentral --cov-append tests/test_full_load_optimization.py
python3 -m pytest --cov=src/py_load_pubmedcentral --cov-append tests/test_full_pipeline.py
python3 -m pytest --cov=src/py_load_pubmedcentral --cov-append tests/test_pipeline_integration.py
python3 -m pytest --cov=src/py_load_pubmedcentral --cov-append tests/test_retractions.py
python3 -m pytest --cov=src/py_load_pubmedcentral --cov-append tests/test_s3_acquisition.py

coverage report
