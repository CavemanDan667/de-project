# de-project

## Overview
This project aims to create a data platform that extracts data from an operational database, archives it in a data lake, and makes it available in a remodelled OLAP data warehouse.
* It operates automatically on a schedule to extract any recently updated data from the existing database.
* This data is stored in an s3 bucket on AWS, and a Cloudwatch log of the process is created.
* Any errors within this process trigger an email alert.
* The addition of data to this s3 bucket triggers the transformation process, in which this data is remodelled and added to the new data warehouse.
* The warehouse has been created to separate data into dimension and fact tables, to improve readability and querying.
* This process is also monitored and errors appropriately logged.
* All Python code is unit tested, PEP8 compliant, and checked for security vulnerabilities with safety and bandit packages.
* These checks run automatically through GitHub Actions.

This project has been deployed through AWS and runs automatically; however, there are instructions below to run the same project locally.

## Contents
Included in this repo: 
1. An `src` directory containing the various python modules of the application, separated by function.
2. A `tests` directory for unit tests of these python functions, along with test data to run them against.
3. A `mock_database` directory containing .sql and .py files to create and seed two test databases.
4. A `terraform` directory containing the modules needed to manage AWS infrastructure.
5. A `package` directory containing files needed for remote terraforming of this project.
6. A `.github/workflows` directory containing YAML files used for CI/CD with GitHub Actions.
7. A `Makefile` containing code to build this project locally.
8. A `diagrams` directory containing .drawio files used during planning stages.
9. This `README` to detail the contents and operating instructions.
10. A `.gitignore` file.
11. A `requirements.txt` file listing all the library dependencies and version numbers.
12. A `.python-version` file, created using `pyenv local`.


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
4. Seed the test databases.
```bash
psql -f mock_database/seed.sql
psql -f mock_database/seed_empty.sql
```
5. Create a .env file to hold config details for creating connections.
-- Necessary content for these files is not available publicly.
6. Check that the unit tests are passing.
```bash
make unit-test
```
7. Check that the code is PEP8 compliant.
```bash
make run-flake
```
8. Terraform the infrastructure.
```bash
terraform init
terraform plan
terraform apply -auto-approve
```