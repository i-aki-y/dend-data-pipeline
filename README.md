# Data Pipelines with Airflow

This is a udacity's data engineer nano-degree project

## About the project

In this project, I make data pipeline which apache-airflow.

The pipeline contains the following processes:

1. Staging data stored S3 to Redshift database.
2. Load data from staging table to fact table of star schema.
3. Load data from staging table to dimension table of star schema.
4. Run some data quality checks

## setup airflow

In order to perform this project, you need to setup apache-airflow.

Instruction of apache-airflow installation is given in the following page.
https://airflow.apache.org/installation.html

You can install `apache-airflow` by using `pip`.

```
pip install apache-airflow
```

If installation finish successfully, you can use airflow command.
In order to run airflow, you should initialize database.

```
airflow initdb
```

With the default setting, the command setup SQLite as airflow database.

When you run the airflow, it create `~/airflow/airflow.cfg` file.
In order to make airflow be able to look your dags and plugin directory,
edit your `airflow.cfg` such as:

```
dags_folder = {PROJECT_ROOT}/dags
plugins_folder = {PROJECT_ROOT}/plugins
```

You should replace `{PROJECT_ROOT}` depending on your environment.

To remove buildin examples, fix the `firflow.cfg`:

```
load_examples = False
```

And run following command.

```
airflow resetdb
```

cf. https://stackoverflow.com/questions/43410836/how-to-remove-default-example-dags-in-airflow

To start airflow, run:

```
$ airflow webserver
$ airflow scheduler
```

## Contents

- `README.md`: this document.
- `dags/`: contains definition of `DAG`s
- `plugins/helpers/`: contains helper functions such as definition of sql used in this project.
- `plugins/operators`: contains definitions of custom operators.
