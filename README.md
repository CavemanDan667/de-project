# de-project
[![Security Checks and Testing](https://github.com/CavemanDan667/de-project/actions/workflows/checks_and_tests.yml/badge.svg)](https://github.com/CavemanDan667/de-project/actions/workflows/checks_and_tests.yml)
[![Terraform Deployment](https://github.com/CavemanDan667/de-project/actions/workflows/terraform_deployment.yml/badge.svg)](https://github.com/CavemanDan667/de-project/actions/workflows/terraform_deployment.yml)

### Contributors
Dan Cox | 
Jason Colville | 
Jack Murphy | 
Peter Konstantynov | 
Helen Lyttle

## Overview
This project creates a data platform that extracts data from an operational database, and archives the ingested data in a data lake.
It then transforms this data, also archiving this as a parquet file, and finally loads it into a remodelled OLAP data warehouse.
* It operates automatically on a schedule to extract any recently updated data from the existing database.
* This data is stored in an s3 bucket on AWS, and a Cloudwatch log of the process is created.
* Any errors within this process trigger an email alert.
* The addition of data to this s3 bucket triggers the transformation process, in which this data is remodelled and stored as a parquet file.
* The addition of transformed data into the second s3 bucket in turn triggers the load process.
* The new warehouse has been created to separate data into dimension and fact tables, to improve readability and querying.
* These transform and load processes are also monitored and errors are appropriately logged.
* All Python code is unit tested, PEP8 compliant, checked for coverage levels, and checked for security vulnerabilities with safety and bandit packages.
* These checks run automatically through GitHub Actions.

This project has been deployed through AWS and runs automatically; however, there are instructions below to run the same project locally.

## Contents
Included in this repo: 
1. An `src` directory containing the various python modules of the application, separated by function.
2. A `tests` directory for unit tests of these python functions, along with test data to run them against.
3. A `mock_databases` directory containing .sql and .py files to create and seed three test databases.
4. A `terraform` directory containing the modules needed to manage AWS infrastructure.
5. A `.github/workflows` directory containing YAML files used for CI/CD with GitHub Actions.
6. A `Makefile` containing code to build this project locally.
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
4. Seed the test databases.
```bash
psql -f mock_databases/seed_mock_db.sql
psql -f mock_databases/seed_empty_db.sql
psql -f mock_databases/seed_mock_dw.sql
```
5. Create a .env file to hold config details for creating connections.
-- Necessary content for these files is not available publicly.
6. Check that the unit tests are passing, and that coverage is at an acceptable level.
```bash
make unit-test
make check-coverage
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