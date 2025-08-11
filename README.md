# User Conversion Rate Spark ETL Job
This project provides a Spark job to calculate the user conversion rate. It processes event tracking data to determine the percentage of users who load the application in the calendar week immediately following their registration week.

## Project Goal
The primary goal is to transform raw event data into a key business metric: **weekly user conversion**. We define this conversion as:

> The percentage of newly registered users who return to load the application for the first time during the calendar week (Monday-Sunday) directly after their registration week.

This metric helps stakeholders understand user activation and engagement shortly after signing up. The ETL pipeline is designed to handle large-scale data spanning multiple years.

---

## Tech Stack
* **PySpark `3.5.2`**: The core ETL library for distributed data processing.
* **Python `3.x`**: The primary programming language.
* **Docker & Docker Compose `3.x`**: For creating consistent, reproducible environments for the application, tests, and development (Jupyter).
* **Taskfile**: A simple, `make`-like build tool for automating common commands (e.g., running the job, tests).
* **pytest**: For robust unit testing of our Spark transformations.
* **pre-commit**: To automatically enforce code style and quality before commits.

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
This task preprocesses the raw event data from `data-input/`. It splits the events into two distinct datasets (user_registration and app_loaded) and writes them to the `data-output/` directory in Parquet format, partitioned by day.

Example output path: `data-output/user_registration/format=parquet/derived_tstamp_day=2020-01-07`

Calculate user conversion rate of one week after registration run:
```
task conversion-rate-week-after-registration
```

This is the main job. It reads the processed data from the previous step and calculates the weekly conversion rate, printing the final metric to the console.
it also writes the weekly user conversion rate back to `data-output/` directory in Parquet format, partitioned by day registration week.

Example output path: `data-output/weekly_summary/format=parquet/registration_week=2020-01`


## Design and Architecture

This project follows standard practices for building scalable and maintainable ETL pipelines.

* Modular Job Structure: The core ETL logic is located in the `jobs/` directory. Each job's data transformation model is isolated in its own file (e.g., user_conversion_rate_model.py), which is then tested independently. It allows easy vertical extension (adding more transformations) and horizontal extension (adding new ETL jobs).

* Automated Code Quality: A pre-commit hook is used to enforce coding standards. It automatically formats Python code before each commit, make code quality consistent the codebase.

* Containerized Environments: Docker and Docker Compose are used for all local development and testing. This provide a consistent environment for all developers, eliminating "it works on my machine" issues and simplifying the path to production deployment.

## Recommended Development Workflow

To ensure accurate data transformations, I recommend a workflow that begins with interactive data exploration before implementing the final ETL logic.

Start with a Notebook

A Jupyter Notebook with PySpark provides a "scratchpad" environment perfect for:

* Interactive Data Exploration: Quickly run queries to understand the structure, distributions, and quality of the source data.

* Rapid Prototyping: Test transformation logic on data samples and instantly validate the results.

* Investigating Anomalies: Efficiently drill down into unexpected patterns or edge cases.

You can launch the project's pre-configured notebook environment by running: `task jupyter`

Example: Insights from the Sample Dataset

Following this exploratory method on the initial dataset revealed several key findings that were critical for building the final job correctly:

* Discovery of Duplicate Events: The initial analysis showed that the same registered events could appear multiple times for a single user. This discovery led directly to adding a deduplication step in the pipeline to ensure accurate user counts.

* Validating Business Logic: I initially observed a 28% conversion rate. By slicing the data in the notebook, we could investigate potential drivers. For instance, we found no strong correlation with the registration channel.

* Identifying Edge Cases: The analysis highlighted that registrations from the 'direct' channel had a 0% conversion rate. While this was a small sample, identifying such anomalies is crucial for asking further business questions and ensuring the pipeline handles all segments correctly.
