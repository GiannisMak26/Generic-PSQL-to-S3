# Code Overview: PostgreSQL connection and S3 Upload of Files

## Introduction
This code provides a set of reusable functions, designed to interecat with a PostgreSQL database, convert the database tables to Parquet files, and uploade those Parquet files to an Amazon S3 bucket. These functions are to be integrated into other Python scripts and used in data processing workflows.

## Functions

### 1. `create_db_engine(user, password, host, port, db_name)`

#### Purpose
Creates an SQLAlchemy engine for connecting to a PostgreSQL database.

#### Parameters
- `user` (str): Database username.
- `password` (str): Database password.
- `host` (str): Database host address.
- `port` (int): Database port number.
- `db_name` (str): Name of the database.

#### Returns
- `engine`: A SQLAlchemy engine object that allows connection to the PostgreSQL database.

#### Example Usage
```python
engine = create_db_engine(user='your_user', password='your_password', host='your_host', port=5432, db_name='your_db_name')
```

### 2. `list_tables(engine, schema)`

#### Purpose
Lists all table names within a specified schema of the PostgreSQL database.

#### Parameters
- `engine` (str): (SQLAlchemy Engine): The engine object created using `create_db_engine`.
- `schema` (str): The name of the schema in the database.

#### Returns
- `tables` (list): A list of table names i the specified schema.

#### Example Usage
```python
tables = list_tables(engine, schema='your_schema_name')
```

### 3. `save_tables_as_parquet(engine, tables, output_folder, schema)`

#### Purpose
Saves the specified database tables as Parquet files in a local directory.

#### Parameters
- `engine ` (SQLAlchemy Engine): The engine object created using `create_db_engine`.
- `tables` (list): A list of table names to be saved.
- `output_folder` (str): The directory where the Parquet files will be stored.
- `schema` (str): The schema name where the tables are located.

#### Example Usage
```python
save_tables_as_parquet(engine, tables, output_folder='/path/to/output', schema='your_schema_name')
```

### 4. `upload_to_s3(aws_access_key_id, aws_secret_access_key, aws_region, bucket_name, folder_name, local_folder)`

#### Purpose
Uploads Parquet files from a specified local directory to an Amazon S3 bucket.

#### Parameters
- `aws_access_key_id` (str): AWS access key ID.
- `aws_secret_access_key` (str): AWS secret access key ID.
- `aws_region` (str): AWS region name
- `bucket_name` (str): The name of the S3 bucket.
- `folder_name` (str): The folder in the S3 bucket where the files will be uploaded.
- `local_folder` (str): The local directory containing Parquet files to upload.

#### Functionality
- **File Filtering:** The function checks the files in the `local_folder` and uploads only those with a `.parquet` extension to the specified S3 bucket.

#### Example Usage
```python
upload_to_s3(
    aws_access_key_id='your_access_key',
    aws_secret_access_key='your_secret_key',
    aws_region='your_region',
    bucket_name='your_bucket_name',
    folder_name='your_folder_name',
    local_folder='/path/to/local/folder'
)
```

## Conclusion
This code is designed to be flexible and reusable, allowing users to easily connect to a PostgreSQL database, extract data tables as Parquet files, and efficiently upload those files to Amazon S3. Each function is parameterized to allow for different configurations, making it adaptable to various use cases in data processing pipelines.