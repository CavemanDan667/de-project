# de-project

* The Makefile is set up with separate tasks to allow for easier integration with GitHub actions.
* It is recommended that, on every pull down to the main branch after a merge of a previous working branch, we run `make run-checks` to confirm that everything is still up-to-date locally and compliant.


## Overview
This project aims to...


## Contents
Included in this repo: 
1. An `src` directory containing the various python modules of the application, separated by function.
2. A `tests` directory for unit tests of these python functions.
3. A `terraform` directory containing the modules needed to manage AWS infrastructure.
4. A `.github/workflows` directory containing YAML files used for CI/CD with GitHub Actions.
5. A `Makefile` containing code to build this project locally.
6. A `diagrams` directory containing .drawio files used during planning stages.
7. This `README` to detail the contents and operating instructions.
8. A `.gitignore` file.
9. A `requirements.txt` file listing all the library dependencies and version numbers.
10. A `.python-version` file, created using `pyenv local`.


## Instructions
To deploy this project locally:
1. Create and activate a virtual environment.
```bash
python -m venv venv
source venv/bin/activate
```
2. Install the dependencies.
```bash
pip install -r requirements.txt
```
3. Set up the `PYTHONPATH`.
```bash
export PYTHONPATH=$(pwd)
```
4. Check that the unit tests are passing.
```bash
pytest -v
```
5. Check that the code is PEP8 compliant.
```bash
flake8  src/ test/
```
6. Terraform the infrastructure.
```bash
terraform init
terraform plan
terraform apply -auto-approve
```