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
	$(call execute_in_env, bandit -lll ./src/*/*/*.py ./src/*/*.py ./tests/*.py ./mock_database/*.py)

## Check for security vulnerabilities with safety
run-safety:
	$(call execute_in_env, safety check -r ./requirements.txt)

## Run all security tests (bandit + safety)
security-test: run-bandit run-safety

## Check code for pep8 compliance with flake8
run-flake:
	$(call execute_in_env, flake8  ./src ./tests/*.py ./mock_database)

## Run unit tests on ingestion utils
test-ingestion:
	$(call execute_in_env, PYTHONPATH=$(shell pwd) pytest -v tests/ingestion_tests --ignore tests/ingestion_tests/test_ingestion.py)

## Run unit tests on process utils
test-process:
	$(call execute_in_env, PYTHONPATH=$(shell pwd) pytest -v tests/process_tests)

## Run all unit tests
unit-test:
	$(call execute_in_env, PYTHONPATH=$(shell pwd) pytest -v --ignore tests/ingestion_tests/test_ingestion.py)

## Run the coverage check
check-coverage:
	$(call execute_in_env, PYTHONPATH=$(shell pwd) coverage run --omit 'venv/*' -m pytest && coverage report -m)

## Run all checks
run-checks: requirements security-test run-flake unit-test check-coverage
