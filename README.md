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

Make sure you have `docker-compose` and [`task`](https://taskfile.dev/#/) installed

Application local run and unit tests are in docker, no other dependencies needed

Unit-tests run:
```
task unit-tests
```

Events split run:
```
task events-split
```

Calculate user conversion rate of one week after registration run:
```
task conversion-rate-week-after-registration
```

## Design and architecture considerations

This project uses a standard pyspark structure, ETL jobs python files are located in the root
of `jobs` folder. For each job, I separate data transformation logics into an independent file,
pyspark unit tests only test data transformation logics. This project structure allows easy vertical
extension -- add new transformation logics and test, and horizontal extension -- add new ETL jobs.

Make usage of the pre-commit hook to ensure coding standard and quality, automatically format python
code before git commit.

Use docker and dock-compose for development and testing, it avoids potential gaps between developpers in
the same project.

Pack the executable in docker container for easier distribution and easier production adoptions.

## My approach and problem encountered.

I used a jupyter notebook with pyspark support as code scatch pad, it notably helps for data explorations, it enables adhoc verifications and tests.

For the second tasks, I discovered that the user registrations data has duplicates entries. The user conversion rate seems low for me, only 28% of registrations are converted to app_loaded, I made a quick
data analyse with jupyter, it does not show any relations between registration channel and conversion rate though, I spotted that all directed registrations has no conversion, but due to its very small number -- 2% of registration in the sample, maybe it is just a coincidence.

the notebook is in `./jupyter-notebook` folder, you can see my explorations by running:
```
task jupyter
```
it starts the jupyter notebook with docker-compose and mount the notebook folder into docker container
