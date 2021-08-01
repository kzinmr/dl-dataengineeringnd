# dl-dataengineeringnd
In this project, we try to build analytics data lake for a Music service. We perform an ETL pipeline with Spark as following:
- Extract JSON log data from S3.
- Transform and load extracted data into fact and dimensional tables with Spark.
- Save the table data in partitioned parquet format on another S3 bucket.

## AWS setup

1. Create IAM role.

2. Setup and create EMR clusters.
  - `$ aws emr create-default-roles --profile <profile-name>`
  - `$ aws emr create-cluster ...`
  - Change Security Groups for the master node to enable ssh from client machines.

3. Create a S3 bucket to save output data.

## ETL pipeline excution

1. Connect to the master node. 

2. Copy the script (`etl.py`) and fill the credential info in the config file (`dl.cfg`).

3. Run `$ spark-submit etl.py`

4. Make sure the output files are saved on S3 bucket.
