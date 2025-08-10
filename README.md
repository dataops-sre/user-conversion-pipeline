# User Conversion Rate Spark ETL Job
This project provides a Spark job to calculate the user conversion rate. It processes event tracking data to determine the percentage of users who load the application in the calendar week immediately following their registration week.

## Project Goal
The primary goal is to transform raw event data into a key business metric: **weekly user conversion**. We define this conversion as:

> The percentage of newly registered users who return to load the application for the first time during the calendar week (Monday-Sunday) directly after their registration week.

This metric helps stakeholders understand user activation and engagement shortly after signing up. The ETL pipeline is designed to handle large-scale data spanning multiple years.

---

## Tech Stack
* **PySpark `3.3.1`**: The core ETL library for distributed data processing.
* **Python `3.10.6`**: The primary programming language.
* **Docker & Docker Compose `2.12.2`**: For creating consistent, reproducible environments for the application, tests, and development (Jupyter).
* **Taskfile**: A simple, `make`-like build tool for automating common commands (e.g., running the job, tests).
* **Pytest & `pytest-spark`**: For robust unit testing of our Spark transformations.
* **Pre-commit**: To automatically enforce code style and quality before commits.

---

## Data & Transformation Logic
The job processes two types of events from a raw source.

### Input Data Schema

1.  **User Registration**: Captures when a user creates an account.
    ```json
    {
        "event": "registered",
        "timestamp": "2023-04-10T10:00:00.000Z",
        "initiator_id": 1,
        "channel": "organic"
    }
    ```
2.  **Application Loaded**: Captures when a user opens the application.
    ```json
    {
        "event": "app_loaded",
        "timestamp": "2023-04-18T14:30:00.000Z",
        "initiator_id": 1,
        "device_type": "mobile"
    }
    ```

### Transformation & Calculation Example

The core logic identifies each user's registration week and then checks for their first `app_loaded` event in the subsequent week.

**Example Scenario**:

Let's assume the following source data:
```json
{"event":"registered", "timestamp":"2020-11-02T06:21:14.000Z",
"initiator_id":1} //week#1: Monday
{"event":"registered", "timestamp":"2020-11-02T07:00:14.000Z",
"initiator_id":2} //week#1: Monday
{"event":"app_loaded", "timestamp":"2020-11-03T06:24:42.000Z",
"initiator_id":1, "device_type":"desktop"} //week#1: Tuesday
{"event":"registered", "timestamp":"2020-11-03T07:00:14.000Z",
"initiator_id":3} //week#1: Wednesday
{"event":"app_loaded", "timestamp":"2020-11-11T10:13:42.000Z",
"initiator_id":2, "device_type":"desktop"} //week#2: Wednesday
{"event":"app_loaded", "timestamp":"2020-11-12T11:08:42.000Z",
"initiator_id":2, "device_type":"desktop"} //week#2: Thursday
{"event":"app_loaded", "timestamp":"2020-11-17T11:08:42.000Z",
"initiator_id":3, "device_type":"mobile"} //week#3: Tuesday
```

**Calculation Steps**:

1.  **Total Unique Registered Users**: We have 3 unique users who registered (`initiator_id`: 1, 2, 3).
2.  **Identify Converted Users**:
    * **User 1**: Registered in Week #1 and loaded the app in Week #1. **This is a conversion.** ✅
    * **User 2**: Registered in Week #1 but their first load was in Week #2. This is not a conversion.
    * **User 3**: Registered in Week #1 but loaded the app in Week #3. This is not a conversion.
3.  **Calculate Conversion Rate**:
    * Total Converted Users = 1
    * Total Registered Users = 3
    * Conversion Rate = $(1 / 3) * 100 \approx 33.33\%$


### Output

The final output is saved with the following schema, providing the weekly conversion rate.
```json
{
  "registration_week": "2023-11",
  "total_registered": 3,
  "total_converted": 1,
  "conversion_rate": 33
}
```

## 📁 Project Structure
```
.
├── Dockerfile
├── Dockerfile.test
├── README.md
├── Taskfile.yml
├── data-input                                # Sample raw input data
│   └── dataset.json
├── data-output
├── docker-compose.yml
├── jobs
│   ├── __init__.py
│   ├── conversion_rate                       # Core Spark ETL logic
│   │   ├── __init__.py
│   │   ├── conversion_rate_week_after_registration_config.py
│   │   └── user_conversion_rate_model.py
│   ├── conversion_rate_week_after_registration.py
│   ├── events_split
│   │   ├── __init__.py
│   │   ├── events_split_config.py
│   │   └── events_split_model.py
│   ├── events_split.py
│   └── unit_tests                            # Unit tests for the ETL job
│       ├── __init__.py
│       ├── test_conversion_rate.py
│       └── test_events_split.py
├── jupyter-notebook                          # Exploration notebook
│   └── user-registrations-and-app-loaded-jupyter.ipynb
├── pytest.ini
├── requirements-test.txt
├── scripts
│   └── entrypoint.sh

```

## Run tasks locally

Make sure you have `docker-compose` and [`task`](https://taskfile.dev/#/) installed:

```
brew install go-task/tap/go-task
brew install docker-compose
```

Application local run and unit tests are in docker, no other dependencies needed, place in root of the project and :

Unit-tests run:
```
task unit-tests
```

Events split run:
```
task events-split
```
Split input event from `data-input` folder, write user_registration and app_loaded data as parquet format to `data-output` folder

Output path format: `data-output/user_registration/format=parquet/derived_tstamp_day=2020-01-07`

Calculate user conversion rate of one week after registration run:
```
task conversion-rate-week-after-registration
```
Take user_registration and app_loaded data generated by `events-split` as input and print the metric in the console.

## Design and architecture considerations

This project uses a standard pyspark structure, ETL jobs python files are located in the root
of `jobs` folder. For each job, I separate data transformation logics into an independent file,
pyspark unit tests only test data transformation logics. This project structure allows easy vertical
extension -- add new transformation logics and tests, and horizontal extension -- add new ETL jobs.

Make usage of the pre-commit hook to ensure coding standard and quality, automatically format python
code before git commit.

Use docker and dock-compose for local development and testing, it avoids potential gaps between developpers in
the same project.

Pack the executable in docker container for easier distribution and easier production adoptions.

## My approach and problems encountered.

I used a jupyter notebook with pyspark support as code scatch pad, it notably helps for data explorations, it enables adhoc verifications and tests.

For the second tasks, I discovered that the user registrations data has duplicates entries.

The user conversion rate seems low, only 28% of registrations are converted to app_loaded, I made a quick data analysis with jupyter, it does not show any relations between registration channel and conversion rate, I spotted that all directed registrations has no conversion, but due to its very small number -- 2% of registration in the sample, maybe it is just a coincidence.

the notebook is in `./jupyter-notebook` folder, you can see my explorations in github or by running:
```
task jupyter
```
it starts the jupyter notebook with docker-compose and mount the notebook folder into docker container
