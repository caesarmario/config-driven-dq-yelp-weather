####
## Utilities for ETL process
## Tech Implementation Answer by Mario Caesar // caesarmario87@gmail.com
####

import os
import json
import traceback
import pandas as pd
import numpy as np
import psycopg2

from pathlib import Path
from io import BytesIO
from datetime import datetime
from minio import Minio
from minio.error import S3Error
from psycopg2 import OperationalError, sql
from psycopg2.extras import execute_values

from utils.logging_utils import logger
from utils.validation_utils import ValidationHelper

class ETLHelper:
    """
    Helper for common ETL tasks
    """
    def create_minio_conn(self, credentials):
        try:
            endpoint         = credentials["MINIO_ENDPOINT"]
            access_key       = credentials["MINIO_ACCESS_KEY"]
            secret_key       = credentials["MINIO_SECRET_KEY"]

            return Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=False
            )
        except Exception as e:
            logger.error(f"!! Failed to create MinIO connection: {e}")
    

    def upload_df_to_minio(self, df, bucket, path, minio_client):
        try:
            buffer = BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)

            minio_client.put_object(
                bucket_name=bucket,
                object_name=path,
                data=buffer,
                length=buffer.getbuffer().nbytes,
                content_type="application/octet-stream"
            )
            logger.info(f">> Uploaded to MinIO: {bucket}/{path}")
        except Exception as e:
            logger.error(f"!! Failed to upload parquet to MinIO: {e}")
            raise


    def download_file_from_minio(self, client, bucket_name, object_name):
        try:
            logger.info(f">> Downloading from MinIO: {bucket_name}/{object_name}")
            response = client.get_object(bucket_name, object_name)

            local_path = f"/tmp/{object_name.replace('/', '_')}"
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            with open(local_path, "wb") as file_data:
                for chunk in response.stream(32 * 1024):
                    file_data.write(chunk)

            logger.info(f">> Successfully downloaded to {local_path}")
            return local_path

        except Exception as e:
            logger.error(f"!! Failed to download file from MinIO: {e}")
            raise

    
    def delete_parquet_chunks(self, client, bucket_name: str, prefix: str):
        """
        Deletes all .parquet files in a given prefix path in the specified MinIO bucket.
        """
        

        try:
            objects_to_delete = client.list_objects(bucket_name, prefix=prefix, recursive=True)
            parquet_objects = [obj.object_name for obj in objects_to_delete if obj.object_name.endswith(".parquet")]

            for obj_name in parquet_objects:
                client.remove_object(bucket_name, obj_name)
                print(f">> Deleted existing chunk: {obj_name}")

            if not parquet_objects:
                print(">> No existing parquet chunks found to delete.")

        except S3Error as err:
            print(f"!! MinIO S3Error while deleting parquet chunks: {err}")
            traceback.print_exc()
            raise

        except Exception as e:
            print(f"!! Unexpected error while deleting parquet chunks: {e}")
            traceback.print_exc()
            raise


    def delete_csv_chunks(self, client, bucket_name: str, prefix: str):
        """
        Deletes all .csv files in a given prefix path in the specified MinIO bucket.
        """
        

        try:
            objects_to_delete = client.list_objects(bucket_name, prefix=prefix, recursive=True)
            parquet_objects = [obj.object_name for obj in objects_to_delete if obj.object_name.endswith(".csv")]

            for obj_name in parquet_objects:
                client.remove_object(bucket_name, obj_name)
                print(f">> Deleted existing chunk: {obj_name}")

            if not parquet_objects:
                print(">> No existing csv chunks found to delete.")

        except S3Error as err:
            print(f"!! MinIO S3Error while deleting csv chunks: {err}")
            traceback.print_exc()
            raise

        except Exception as e:
            print(f"!! Unexpected error while deleting parquet chunks: {e}")
            traceback.print_exc()
            raise

    
    def upload_file_to_minio(self, client, bucket_name: str, object_name: str, local_file_path: str):
        try:
            if not os.path.exists(local_file_path):
                raise FileNotFoundError(f"File not found: {local_file_path}")

            logger.info(f">> Uploading to MinIO: {bucket_name}/{object_name}")
            with open(local_file_path, "rb") as file_data:
                file_stat = os.stat(local_file_path)
                client.put_object(
                    bucket_name=bucket_name,
                    object_name=object_name,
                    data=file_data,
                    length=file_stat.st_size,
                    content_type="application/octet-stream"
                )
            logger.info(f">> Successfully uploaded: {object_name}")

        except S3Error as e:
            logger.error(f"!! MinIO S3Error while uploading {local_file_path}: {e}")
            raise
        except Exception as e:
            logger.error(f"!! Failed to upload file to MinIO: {e}")
            raise


    def read_csv(self, path):
        try:
            df = pd.read_csv(path)
            return df
        except Exception as e:
            logger.error(f"!! Failed to read csv: {e}")


    def load_schema_config(self, subfolder, config_name):
        try:
            with open(f'schema_config/{subfolder}/{config_name}_config.json', 'r') as file:
                return json.load(file)
        except Exception as e:
            logger.error(f"!! Failed to load schema config cols: {e}")


    def transform_to_date(self, value):
        try:
            return pd.to_datetime(str(value), format="%Y%m%d", errors="coerce")
        except Exception:
            return pd.NaT


    def to_numeric(self, value):
        try:
            return pd.to_numeric(value, errors='coerce')
        except Exception:
            return np.nan
        


    def apply_transformation(self, df, config):
        for col, meta in config.items():
            transform_func = meta.get("transformation")
            if transform_func == "transform_to_date":
                df[col] = df[meta["col_csv"]].apply(self.transform_to_date)
            elif transform_func == "to_numeric":
                df[col] = df[meta["col_csv"]].apply(self.to_numeric)
            else:
                df[col] = df[meta["col_csv"]]
        return df
    

    def apply_validation(self, df, config):
        df["is_valid"] = True
        df["error_reason"] = ""

        validation_helper = ValidationHelper()

        for col, meta in config.items():
            validations = meta.get("validation", [])
            if isinstance(validations, str):
                validations = [validations]

            for rule in validations:
                if hasattr(validation_helper, rule):
                    validation_func = getattr(validation_helper, rule)
                    try:
                        result = validation_func(df[col])
                        failed = ~result
                        df.loc[failed, "is_valid"] = False
                        df.loc[failed, "error_reason"] = df.loc[failed, "error_reason"].apply(
                            lambda x: ("" if pd.isnull(x) else str(x)) + f"{col}_{rule};"
                        )
                    except Exception as e:
                        logger.error(f"!! Validation rule failed: {col}.{rule} - {e}")
                else:
                    logger.warning(f">> Skipping unknown column validator: {rule}")

        return df
    
    def read_parquet(self, bucket_name, credentials, folder_name, file_name):
        try:
            # Construct the path to the Parquet file
            path = (
                f"{folder_name}_dataset/{file_name}/yelp_academic_dataset_{file_name}_valid.parquet"
                if folder_name == "yelp"
                else f"{folder_name}/{file_name}/{file_name}_valid.parquet"
            )

            minio_client = self.create_minio_conn(credentials)
            resp         = minio_client.get_object(bucket_name, path) # Fetch the Parquet file from the MinIO bucket

            byte_data    = BytesIO(resp.read()) # Read the Parquet data into memory
            df           = pd.read_parquet(byte_data)

            return df

        except Exception as e:
            logger.error(f"!! Failed to read Parquet from MinIO bucket '{bucket_name}', path '{path}': {e}")
            raise
            
        finally:
            if 'resp' in locals() and resp is not None:
                try:
                    resp.close()
                    resp.release_conn()
                except Exception:
                    pass


    def read_parquet_chunks(self, bucket_name, credentials, folder_name, file_name_prefix):
        try:
            minio_client = self.create_minio_conn(credentials)
            prefix = f"{folder_name}_dataset/{file_name_prefix}/"

            # Dapatkan semua file parquet di prefix tersebut
            chunk_paths = [
                obj.object_name for obj in minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
                if obj.object_name.endswith(".parquet") and not obj.object_name.endswith(f"{file_name_prefix}.parquet")
            ]

            if not chunk_paths:
                raise ValueError(f"No parquet chunks found for {file_name_prefix}")

            return chunk_paths

        except Exception as e:
            logger.error(f"!! Failed to list parquet chunks from MinIO: {e}")
            raise

    
    def create_postgre_conn(self, db_creds):
        try:
            user           = db_creds["POSTGRES_USER"]
            password       = db_creds["POSTGRES_PASSWORD"]
            host           = db_creds["POSTGRES_HOST"]
            port           = db_creds["POSTGRES_PORT"]
            dbname         = db_creds["POSTGRES_DB"]

            conn = psycopg2.connect(
                database=dbname,
                user=user,
                password=password,
                host=host,
                port=port
            )

            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                logger.info(">> Successfully connected to PostgreSQL database.")
            return conn

        except Exception as e:
            logger.error(f"!! Creating postgres connection failed: {e}")
            raise


    def load_config(self, subfolder, config_name):
        with open(f'schema_config/{subfolder}_schema_config/{config_name}.json', 'r') as file:
            return json.load(file)


    def load_reserved_keywords(self):
        try:
            # Load reserve keywords
            with open(f'scripts/utils/reserved_keywords.json', 'r') as file:
                return json.load(file)["postgres_reserved_keywords"]
        except Exception as e:
            logger.error(f"!! Failed to load reserve keywords: {e}")
            raise

    
    def _map_dtype_to_postgres(self, dtype):
        dtype_mapping = {
            'string': 'TEXT',
            'datetime64[ns]': 'TIMESTAMP',
            'float64': 'DOUBLE PRECISION',
            'bool': 'BOOLEAN',
            'int64': 'INTEGER',
            'object': 'TEXT',
            'datetime': 'TIMESTAMP',
        }
        
        return dtype_mapping.get(str(dtype), 'TEXT')
    

    def _map_dtype_to_postgres_from_config(self, column_data_type):
        dtype_mapping = {
            'STRING': 'TEXT',
            'DATE': 'DATE',
            'DATETIME': 'TIMESTAMP',
            'FLOAT': 'DOUBLE PRECISION',
            'BOOLEAN': 'BOOLEAN',
            'INTEGER': 'INTEGER',
        }
        
        return dtype_mapping.get(column_data_type, 'TEXT')
    

    def alter_table_schema(self, conn, table_name: str, schema: str, df: pd.DataFrame):
        try:
            with conn.cursor() as cursor:
                # Get existing columns and data types from the database
                cursor.execute(
                    sql.SQL("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s AND table_schema = %s"),
                    [table_name, schema]
                )
                existing_columns = {row[0]: row[1] for row in cursor.fetchall()}

                # Get new columns from DataFrame
                new_columns = df.columns.tolist()

                # Add new columns or alter existing columns
                for column in new_columns:
                    pg_data_type = self._map_dtype_to_postgres(df[column].dtype)

                    if column not in existing_columns:
                        logger.info(f"Adding new column {column} with type {pg_data_type} to {schema}.{table_name}.")
                        cursor.execute(
                            sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} {};").format(
                                sql.Identifier(schema),
                                sql.Identifier(table_name),
                                sql.Identifier(column),
                                sql.SQL(pg_data_type)
                            )
                        )
                        conn.commit()
                        logger.info(f"Column {column} added to {schema}.{table_name}.")
                    elif existing_columns[column].upper() != pg_data_type.upper():
                        logger.info(f"Updating column {column} from {existing_columns[column]} to {pg_data_type}.")
                        cursor.execute(
                            sql.SQL("ALTER TABLE {}.{} ALTER COLUMN {} SET DATA TYPE {};").format(
                                sql.Identifier(schema),
                                sql.Identifier(table_name),
                                sql.Identifier(column),
                                sql.SQL(pg_data_type)
                            )
                        )
                        conn.commit()
                        logger.info(f"Column {column} updated to {pg_data_type} in {schema}.{table_name}.")

        except Exception as e:
            logger.error(f"Error in altering table schema for {schema}.{table_name}: {e}")
            raise


    def check_and_create_table(self, conn, table_name: str, schema: str, df: pd.DataFrame):
        try:
            # Create a cursor to execute SQL queries
            with conn.cursor() as cursor:
                
                # Check if the schema exists in the database
                cursor.execute(
                    sql.SQL("SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = %s)"),
                    [schema]
                )
                schema_exists = cursor.fetchone()[0]  # Fetch the result of the query, which is a boolean

                if not schema_exists:
                    logger.info(f"Schema {schema} does not exist. Creating schema...")
                    try:
                        cursor.execute(
                            sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema))
                        )
                        conn.commit()
                        logger.info(f"Schema {schema} created successfully.")
                    except Exception as e:
                        logger.error(f"Error creating schema {schema}: {e}")
                        if "duplicate key" in str(e):  # Ignore error if schema already exists
                            logger.info(f"Schema {schema} already exists. Skipping creation.")

                # Check if the table exists in the schema by querying the information_schema.tables
                cursor.execute(
                    sql.SQL("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s AND table_schema = %s)"),
                    [table_name, schema]
                )
                table_exists = cursor.fetchone()[0]  # Fetch the result of the query, which is a boolean
                
                if not table_exists:
                    logger.info(f"Table {schema}.{table_name} does not exist. Creating table...")

                    # Generate the SQL columns based on the config file and pandas DataFrame
                    config = self.load_config(schema, f"{table_name}_config")

                    # Load reserved keywords
                    reserved_keywords = self.load_reserved_keywords()

                    columns = []
                    for column, column_config in config.items():
                        # Get the column name and its data type from the config

                        column_name = f'"{column}"' if column.lower() in reserved_keywords else column
                        column_data_type = column_config['dtype']
                        
                        # Map the data type to a PostgreSQL type
                        pg_data_type = self._map_dtype_to_postgres_from_config(column_data_type)
                        
                        # Add the column to the list of SQL columns
                        columns.append(f"{column_name} {pg_data_type}")
                        
                    
                    columns.append("load_dt TIMESTAMP")

                    # Print the columns first (before joining)
                    columns_sql = ", ".join(columns)

                    # Create the SQL query to create the table
                    create_table_query = f"CREATE TABLE {schema}.{table_name} ({columns_sql})"
                    logger.info(f"Generated SQL Query: {create_table_query}")

                    # Execute the create table query
                    cursor.execute(create_table_query)
                    conn.commit()
                    logger.info(f"Table {schema}.{table_name} created successfully. Proceed to next step")
                else:
                    # Checking schema changes if exists
                    logger.info(f"Table {schema}.{table_name} already exists. Checking for schema changes...")
                    self.alter_table_schema(conn, table_name, schema, df)

        except Exception as e:
            logger.error(f"Error in checking or creating table {schema}.{table_name}: {e}")
            raise

    
    def upsert_data_into_table(self, conn, table_name: str, schema: str, df: pd.DataFrame):
        try:
            exec_date = datetime.now().strftime("%Y%m%d")
            temp_table_name = f"{table_name}_temp_{exec_date}"
            load_dt = datetime.now()

            # Tambahkan kolom load_dt ke dataframe
            df["load_dt"] = load_dt

            for col in df.columns:
                if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                    df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

            with conn.cursor() as cursor:
                config = self.load_config(schema, f"{table_name}_config")
                reserved_keywords = self.load_reserved_keywords()

                # Create column definitions
                columns = []
                for column, column_config in config.items():
                    column_name = f'"{column}"' if column.lower() in reserved_keywords else column
                    column_data_type = column_config['dtype']
                    pg_data_type = self._map_dtype_to_postgres_from_config(column_data_type)
                    columns.append(f"{column_name} {pg_data_type}")
                columns.append("load_dt TIMESTAMP")

                delete_temp_table_query = f"DROP TABLE IF EXISTS {schema}.{temp_table_name};"
                cursor.execute(delete_temp_table_query)
                logger.info(f"> Dropping existing temp table: {delete_temp_table_query.strip()}")

                create_temp_table_query = f"CREATE TABLE {schema}.{temp_table_name} ({', '.join(columns)});"
                logger.info(f"> Creating temp table: {create_temp_table_query.strip()}")
                cursor.execute(create_temp_table_query)

                # Insert into temp table
                insert_query = f"INSERT INTO {schema}.{temp_table_name} ({', '.join(df.columns)}) VALUES %s"
                insert_values = [tuple(row) for row in df.values]
                logger.info("> Inserting into temp table...")
                execute_values(cursor, insert_query, insert_values)

                delete_query = f"DELETE FROM {schema}.{table_name} WHERE load_dt < %s"
                logger.info(f"> Deleting old records for load_dt < {load_dt}")
                cursor.execute(delete_query, [load_dt])

                # Insert from temp to main table
                column_list = ", ".join(df.columns)
                insert_main_query = f"""
                    INSERT INTO {schema}.{table_name} ({column_list})
                    SELECT {column_list}
                    FROM {schema}.{temp_table_name};
                """
                cursor.execute(insert_main_query)

                # Drop temp table
                cursor.execute(f"DROP TABLE IF EXISTS {schema}.{temp_table_name};")

            conn.commit()
            logger.info(f">> Upsert process complete for {schema}.{table_name}")

        except Exception as e:
            logger.error(f"!! Error during upsert for {schema}.{table_name}: {e}")
            raise
            

    def execute_sql_to_fact_table(self, conn, schema: str, table_name: str, sql_path: str):
        try:
            exec_date = datetime.now().strftime("%Y%m%d_%H%M%S")
            temp_table_name = f"{table_name}_temp_{exec_date}"

            # Load SQL & Config
            with open(sql_path, 'r') as f:
                sql_content = f.read()

            config = self.load_config(schema, f"{table_name}_config")
            reserved_keywords = self.load_reserved_keywords()

            with conn.cursor() as cursor:
                # Ensure schema exists
                cursor.execute(sql.SQL("SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = %s)"), [schema])
                schema_exists = cursor.fetchone()[0]
                if not schema_exists:
                    cursor.execute(sql.SQL("CREATE SCHEMA {};").format(sql.Identifier(schema)))
                    conn.commit()
                    logger.info(f"> Schema {schema} created")
                else:
                    logger.info(f"> Schema {schema} already exists")

                # Check if main table exists
                cursor.execute(sql.SQL("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s AND table_schema = %s)"), [table_name, schema])
                table_exists = cursor.fetchone()[0]

                if not table_exists:
                    # Build CREATE TABLE for main table
                    columns = []
                    for column, meta in config.items():
                        col_name = f'"{column}"' if column.lower() in reserved_keywords else column
                        pg_type = self._map_dtype_to_postgres_from_config(meta["dtype"])
                        columns.append(f"{col_name} {pg_type}")

                    create_table_sql = f"CREATE TABLE {schema}.{table_name} ({', '.join(columns)});"
                    cursor.execute(create_table_sql)
                    conn.commit()
                    logger.info(f"> Main table {schema}.{table_name} created")
                else:
                    logger.info(f"> Main table {schema}.{table_name} already exists")

                # Create temp table
                temp_columns = []
                for column, meta in config.items():
                    col_name = f'"{column}"' if column.lower() in reserved_keywords else column
                    pg_type = self._map_dtype_to_postgres_from_config(meta["dtype"])
                    temp_columns.append(f"{col_name} {pg_type}")

                create_temp_sql = f"CREATE TABLE {schema}.{temp_table_name} ({', '.join(temp_columns)});"
                cursor.execute(create_temp_sql)
                logger.info(f"> Temp table {temp_table_name} created")

                # Insert data into temp table using external SQL
                insert_temp_sql = f"""
                    INSERT INTO {schema}.{temp_table_name}
                    {sql_content.strip()}
                """
                cursor.execute(insert_temp_sql)
                logger.info(f"> Inserted data into temp table {temp_table_name}")

                # Truncate main table before insert
                cursor.execute(sql.SQL("TRUNCATE TABLE {}.{};").format(
                    sql.Identifier(schema), sql.Identifier(table_name)
                ))
                logger.info(f"> Truncated main table {schema}.{table_name}")

                # Insert from temp to main
                insert_main_sql = f"""
                    INSERT INTO {schema}.{table_name}
                    SELECT * FROM {schema}.{temp_table_name};
                """
                cursor.execute(insert_main_sql)
                logger.info(f"> Moved data from temp to main table")

                # Drop temp table
                cursor.execute(sql.SQL("DROP TABLE IF EXISTS {}.{};").format(
                    sql.Identifier(schema), sql.Identifier(temp_table_name)
                ))
                logger.info(f"> Dropped temp table {temp_table_name}")

            conn.commit()
            logger.info(f">> Fact table {schema}.{table_name} successfully refreshed.")

        except Exception as e:
            logger.error(f"!! Error executing SQL to fact table {schema}.{table_name}: {e}")
            raise
