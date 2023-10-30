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
	bandit -lll ./src/*.py ./tests/*.py

## Check for security vulnerabilities with safety
run-safety:
	safety check -r ./requirements.txt

## Run all security tests (bandit + safety)
security-test: run-bandit run-safety

## Check code for pep8 compliance with flake8
run-flake:
	flake8  ./src/*.py ./tests/*.py

## Run the unit tests
unit-test:
	PYTHONPATH=$(pwd) pytest -v

## Run the coverage check
check-coverage:
	PYTHONPATH=$(pwd) coverage run --omit 'venv/*' -m pytest && coverage report -m

## Run all checks
run-checks: requirements security-test run-flake unit-test check-coverage
