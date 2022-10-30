# User registration and app loaded.

This Project runs Spark job to calculate user conversion rate of one week after registration

Tech stack
* pyspark 3.3.1 -> ETL library
* Python 3.10.6
* Pytest and pytest-spark for unit tests
* pre-commit hook to keep code sanity
* docker-compose 2.12.2 -> for app run, unit-test and jupyter-notebook
* Taskfile -> A makefile alike build tool

## Run tasks locally

Make sure you have `docker-compose` and `task`(https://taskfile.dev/#/) installed

Application local run and unit tests are in docker, no other dependencies needed

Unit-tests run: ```task unit-tests```

Events split run: ```task events-split```

Calculate user conversion rate of one week after registration run: ```task conversion-rate-week-after-registration```

## How did I dev this app ?

I used a [jupyter notebook image](https://hub.docker.com/r/jupyter/pyspark-notebook) with pyspark support as code scatch pad, the notebook is in `./jupyter-notebook` folder, you can see my dev process by run:
```
task jupyter
```
it starts the jupyter notebook with docker-compose and mount the notebook folder into docker

## Design And architecture

it's a simple spark ETL job. I separate ETL
