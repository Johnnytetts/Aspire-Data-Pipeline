import snowflake.connector
import pandas as pd
import os
import glob
import winreg

def get_registry_env_variable(variable_name):
    """Retrieve an environment variable from the Windows registry."""
    try:
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SYSTEM\CurrentControlSet\Control\Session Manager\Environment") as key:
            value, _ = winreg.QueryValueEx(key, variable_name)
            return value
    except FileNotFoundError:
        return None

def create_snowflake_connection():
    """Create a connection to Snowflake using environment variables from the registry."""
    try:
        conn = snowflake.connector.connect(
            user=get_registry_env_variable('SNOWFLAKE_USER'),
            password=get_registry_env_variable('SNOWFLAKE_PASSWORD'),
            account=get_registry_env_variable('SNOWFLAKE_ACCOUNT'),
            warehouse=get_registry_env_variable('SNOWFLAKE_WAREHOUSE'),
            database=get_registry_env_variable('SNOWFLAKE_DATABASE'),
            schema=get_registry_env_variable('SNOWFLAKE_SCHEMA')
        )
        return conn
    except Exception as e:
        print(f"Failed to connect to Snowflake: {e}")
        return None

def get_existing_columns(cursor, table_name):
    """Retrieve existing columns of a Snowflake table."""
    cursor.execute(f"DESCRIBE TABLE {table_name}")
    columns = [row[0].upper() for row in cursor.fetchall()]  # Normalize column names to uppercase
    return columns

def alter_table_to_add_columns(cursor, table_name, new_columns):
    """Alter the table to add new columns if they don't already exist."""
    existing_columns = get_existing_columns(cursor, table_name)
    for column in new_columns:
        if column.upper() not in existing_columns:  # Check in uppercase
            try:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column} STRING")
                print(f"Added column {column} to table {table_name}")
            except snowflake.connector.errors.ProgrammingError as e:
                if "already exists" in str(e):
                    print(f"Column {column} already exists in table {table_name}")
                else:
                    print(f"An error occurred while adding column {column} to table {table_name}: {e}")
        else:
            print(f"Column {column} already exists in table {table_name}")

def create_table_sql(table_name, df):
    """Generate SQL to create a table based on DataFrame columns."""
    columns = ', '.join([f"{col} STRING" for col in df.columns])
    return f"""
        CREATE OR REPLACE TABLE {table_name} (
            {columns}
        );
    """

def clear_stage(stage_name, conn):
    """Clear the Snowflake stage."""
    cursor = conn.cursor()
    try:
        cursor.execute(f"DROP STAGE IF EXISTS {stage_name}")
        print(f"Stage {stage_name} dropped.")

        cursor.execute(f"CREATE STAGE {stage_name}")
        print(f"Stage {stage_name} created.")
    except Exception as e:
        print(f"An error occurred while clearing the stage: {e}")
    finally:
        cursor.close()

def upload_to_stage(file_path, stage_name, conn):
    """Upload CSV file to Snowflake stage."""
    cursor = conn.cursor()
    try:
        file_path = file_path.replace("\\", "/")
        upload_sql = f"""
            PUT 'file://{file_path}' @{stage_name}
        """
        cursor.execute(upload_sql)
    except Exception as e:
        print(f"An error occurred during file upload: {e}")
    finally:
        cursor.close()

def load_csv_to_snowflake(file_path, table_name, stage_name):
    """Load CSV data into Snowflake."""
    df = pd.read_csv(file_path, low_memory=False)
    
    conn = create_snowflake_connection()
    if conn is None:
        return
    
    cursor = conn.cursor()
    
    try:
        # Check if table exists
        table_exists = False
        try:
            cursor.execute(f"DESCRIBE TABLE {table_name}")
            table_exists = True
        except snowflake.connector.errors.ProgrammingError as e:
            if "does not exist" in str(e):
                table_exists = False
            else:
                raise
        
        if table_exists:
            # Drop the table if it exists
            cursor.execute(f"DROP TABLE {table_name}")
            print(f"Table {table_name} dropped.")
        
        # Create new table
        create_sql = create_table_sql(table_name, df)
        cursor.execute(create_sql)
        
        # Clear the stage
        clear_stage(stage_name, conn)
        
        # Upload the file to the stage
        upload_to_stage(file_path, stage_name, conn)
        
        # Load data from the stage into the table
        copy_sql = f"""
            COPY INTO {table_name}
            FROM @{stage_name}/{os.path.basename(file_path)}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER = 1 
                            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE) ON_ERROR = 'CONTINUE' FORCE = TRUE;
        """
        cursor.execute(copy_sql)
        
        result = cursor.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        row_count = result[0]
        print(f"Data from {file_path} loaded into Snowflake table {table_name}. Rows inserted: {row_count}.")
        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cursor.close()
        conn.close()

def process_csv_files(directory, stage_name):
    """Process all CSV files in the directory."""
    for file_path in glob.glob(os.path.join(directory, '*.csv')):
        file_name = os.path.basename(file_path)
        table_name = os.path.splitext(file_name)[0].upper()
        load_csv_to_snowflake(file_path, table_name, stage_name)

# Main logic
if __name__ == "__main__":
    try:
        directory = os.path.dirname(os.path.abspath(__file__))
        stage_name = 'MY_STAGE'
        process_csv_files(directory, stage_name)
    except Exception as e:
        print(f"An error occurred: {e}")