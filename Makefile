## Create python interpreter environment
create-environment:
	@echo ">>> check python3 version"
	( \
		python --version; \
	)
	@echo ">>> Setting up VirtualEnv."
	( \
	    pip install -q virtualenv virtualenvwrapper; \
	    virtualenv venv; \
	)

ACTIVATE_ENV := . venv/bin/activate

define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

## Build the environment requirements
requirements: create-environment
	$(call execute_in_env, pip install -r ./requirements.txt)

## Check for security issues with bandit
run-bandit:
	$(call execute_in_env, bandit -lll ./src/*/*/*.py ./src/*/*.py ./tests/*/*.py ./mock_database/*.py ./mock_data_warehouse/*.py)

## Check for security vulnerabilities with safety
run-safety:
	$(call execute_in_env, safety check -r ./requirements.txt)

## Run all security tests (bandit + safety)
security-test: run-bandit run-safety

## Check code for pep8 compliance with flake8
run-flake:
	$(call execute_in_env, flake8  ./src ./tests)

## Run unit tests on ingestion utils
test-ingestion:
	$(call execute_in_env, PYTHONPATH=$(shell pwd) pytest -v tests/ingestion_tests --ignore tests/ingestion_tests/test_ingestion.py)

## Run unit tests on process utils
test-process:
	$(call execute_in_env, PYTHONPATH=$(shell pwd) pytest -v tests/process_tests)

## Run unit tests on loading utils
test-loading:
	$(call execute_in_env, PYTHONPATH=$(shell pwd) pytest -v tests/load_tests)

## Run all unit tests
unit-test: test-ingestion test-process test-loading

## Run coverage check on ingestion
check-coverage-ingestion:
	$(call execute_in_env, PYTHONPATH=$(shell pwd) coverage run -m pytest tests/ingestion_tests && coverage report -m)

## Run coverage check on process
check-coverage-process:
	$(call execute_in_env, PYTHONPATH=$(shell pwd) coverage run -m pytest tests/process_tests && coverage report -m)

## Run coverage check on load
check-coverage-load:
	$(call execute_in_env, PYTHONPATH=$(shell pwd) coverage run -m pytest tests/load_tests && coverage report -m)

## Run the complete coverage check
check-coverage: check-coverage-ingestion check-coverage-process check-coverage-load

## Run all checks
run-checks: requirements security-test run-flake unit-test check-coverage

## Re-seed the mock database
seed-db:
	$(call execute_in_env, PYTHONPATH=$(shell pwd) psql -f mock_database/seed.sql)
	$(call execute_in_env, PYTHONPATH=$(shell pwd) psql -f mock_database/seed_empty.sql)

## Re-seed the mock data warehouse
seed-dw:
	$(call execute_in_env, PYTHONPATH=$(shell pwd) psql -f mock_data_warehouse/create_dw.sql)