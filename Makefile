## Create python virtual environment
create-environment:
	python -m venv venv

## Build the environment requirements
requirements: create-environment
	source venv/bin/activate
	pip install -r ./requirements.txt

## Check for security issues with bandit
run-bandit:
	bandit -lll */*.py *c/*/*.py

## Check for security vulnerabilities with safety
run-safety:
	safety check -r ./requirements.txt

## Run all security tests (bandit + safety)
security-test: run-bandit run-safety

## Check code for pep8 compliance with flake8
run-flake:
	flake8  ./src/*/*.py ./test/*/*.py

## Run the unit tests
unit-test:
	PYTHONPATH=$(pwd) pytest -v

## Run the coverage check
check-coverage:
	PYTHONPATH=$(pwd) coverage run --omit 'venv/*' -m pytest && coverage report -m

## Run all checks
run-checks: security-test run-flake unit-test check-coverage
