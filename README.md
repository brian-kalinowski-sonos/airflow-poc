# Airflow

Airflow is a tool used to schedule and monitor workflows.

Workflow are a way to accomplish a sequence of tasks ( get data from upstream source, process, put data to a downstream source )

Tasks, can be thoughts of as independent idempotent(sort of ideally) series of steps that together form task.

Operators are there to accomplish the tasks. Several different types of operators are present for our purposes we have the following useful operators.

1. Bash Operators
2. Python Operators
3. Snowflake Operators
4. AWS Operators - S3 Hooks
5. MSSQL Operators

## Dynamic Dag Creation

Let us assume that we have a external process that generates the YAML file. We then pass through the YAML file for config details which we parse in `dynamic-dag-creation.py`.

`dynamic-dag-creation` can be thought of as a skeleton where we decide the flow of a general workflow.

So once we have the `generated-yaml.yaml` file we can dynamically create a dag based off of the skeleton we decided in the `dynamic-dag-file`.


