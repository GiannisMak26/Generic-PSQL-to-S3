# Import necessary libraries
import os
import json
import psycopg
import pandas as pd
import sqlalchemy
import boto3
from sqlalchemy import inspect, MetaData, Table


# Function to create a database connection engine
def create_db_engine(user: str, password: str, host: str, port: str, db_name: str):
    """
    Creates an SQLAclhemey engine to connect to a PostgreSQL database.

    Parameters
    ----------
    user : The username to connect to the database.
    password : The password to connect to the database.
    host : The host of the database.
    port : The port of the database.
    db_name : The name of the database.

    Returns
    -------
    SQLAlchemy engine object
    """
    
    connection_string = f'postgresql+psycopg://{user}:{password}@{host}:{port}/{db_name}'
    return sqlalchemy.create_engine(connection_string)



# Function to list tables in the specified schema
def list_tables(engine, schema):
    """
    Lists all tables in the specified schema.

    Parameters
    ----------
    engine : SQLAlchemy engine object
    schema : The schema to list tables from.

    Returns
    -------
    List of table names in the specified schema
    """
    
    inspector = inspect(engine)
    return inspector.get_table_names(schema=schema)



# Function to save tables as Parquet files
def save_tables_as_parquet(engine, tables:str, output_folder: str, schema: str):
    """
    Saves specified tables from the database as Parquet files.

    Parameters
    ----------
    engine : SQLAlchemy engine object
    tables : List of table names to save as Parquet files.
    output_folder : The folder to save the Parquet files.
    schema : The schema of the tables to save.
    """
    
    os.makedirs(output_folder, exist_ok=True)
    inspector = sqlalchemy.inspect(engine)
    tables = inspector.get_table_names(schema=schema)
    for table_name in tables:
        table = Table(table_name, MetaData(), autoload_with=engine, schema=schema)
        df = pd.read_sql(table.select(), con=engine)
        parquet_filename = os.path.join(output_folder, f"{table_name}.parquet")
        df.to_parquet(parquet_filename)


# Function to upload files to S3
def upload_to_s3(aws_access_key_id, aws_secret_access_key, aws_region, bucket_name, folder_name, local_folder):
    """
    Uploads all the parquet files from the local folder to the specified S3 bucket.

    Parameters
    ----------
    aws_access_key_id : The AWS access key ID of the user.
    aws_secret_access_key : The AWS secret access key of the user.
    aws_region : The AWS region of the S3 bucket.
    bucket_name : The name of the S3 bucket.
    folder_name : The folder in the S3 bucket to upload the files.
    local_folder : The local folder containing the parquet files.
    """
    
    # Initialize AWS session and S3 resource
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
    )
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket_name)
    for filename in os.listdir(local_folder):
        if filename.endswith('.parquet'):
            local_path = os.path.join(local_folder, filename)
            s3_path = f'{folder_name}/{filename}'
            bucket.upload_file(local_path, s3_path)