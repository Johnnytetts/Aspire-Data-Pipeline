# Aspire-Data-Pipeline
 This project was my first data pipeline effort to extract client data from a CRM tool (Aspire) and save extracted data to a cloud storage and then create a second process to manipulate the results into tables and load into a snowflake warehouse to be used for a reporting mart.


Overview
This project includes three key Python scripts designed to automate tasks related to API interaction, data retrieval, and loading data into Snowflake. The scripts work together to refresh authentication tokens, fetch data from various API endpoints, and load CSV data into Snowflake, a cloud-based data warehousing platform.
Prerequisites
Before running any of the scripts, ensure you have the following:
- Python 3.x installed on your machine.
- Required Python libraries:
   requests
   snowflake-connector-python
   pandas
You can install these libraries using the following command(s):
pip install requests
pip install snowflake-connector-python
pip install pandas

Environment Variables
These scripts rely on environment variables stored in the Windows registry. The variables are used for storing sensitive information such as authentication tokens and Snowflake connection details.
Required Environment Variables
-	REFRESH_TOKEN: The current refresh token used to request new tokens.
-		AUTH_TOKEN: The current authentication token (this will be updated by the Token Refresh script).
-	SNOWFLAKE_USER: Snowflake username.
-	SNOWFLAKE_PASSWORD: Snowflake password.
-	SNOWFLAKE_ACCOUNT: Snowflake account name.
-	SNOWFLAKE_WAREHOUSE: Snowflake warehouse name.
-	SNOWFLAKE_DATABASE: Snowflake database name.
-	SNOWFLAKE_SCHEMA: Snowflake schema name.
Setting Up Your Environment Variables
To set up your environment variables in the Windows registry:
-	Open the Windows Registry Editor (regedit).
-	Navigate to Computer\HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Control\Session Manager\Environment.
-	Right-click and select "New" > "String Value" to create a new environment variable.
-	Name the variable as listed above and set its value accordingly.
Ensure that the script is run with administrative privileges to access and modify the registry.
Script Descriptions
- Token Refresh Script (tokenrefresh.py)
This script refreshes authentication tokens by making a request to a specified API endpoint. It retrieves the current refresh token from the Windows registry, requests new tokens, and updates the registry with the new tokens.
Functions
•	is_admin(): Checks if the script is running with administrative privileges.
•	run_as_admin(): Relaunches the script with administrative privileges if it is not already running as an administrator.
•	get_registry_env_variable(variable_name): Retrieves an environment variable from the Windows registry.
•	set_registry_env_variable(variable_name, value): Sets an environment variable in the Windows registry.
•	refresh_tokens(): Requests new tokens using the current refresh token and updates the registry with the new tokens.
- Data Fetching Script (main.py)
This script fetches data from various API endpoints specified in the endpoints_with_ids dictionary and saves the data to CSV files.
Functions
•	fetch_data_from_endpoint(endpoint_name): Fetches data from a specified API endpoint.
•	save_data_to_csv(data, endpoint_name): Saves the fetched data to a CSV file in the specified directory.
•	main(): Main function to iterate through the endpoints and fetch data.
- Snowflake Loading Script (snow_load.py)
This script loads CSV data into Snowflake. It connects to Snowflake using details retrieved from the Windows registry, creates tables, and uploads the data.
Functions
•	create_snowflake_connection(): Creates a connection to Snowflake using environment variables from the registry.
•	get_existing_columns(cursor, table_name): Retrieves the existing columns of a Snowflake table.
•	alter_table_to_add_columns(cursor, table_name, new_columns): Alters a table to add new columns if they don't already exist.
•	create_table_sql(table_name, df): Generates SQL to create a table based on DataFrame columns.
•	clear_stage(stage_name, conn): Clears the Snowflake stage.
•	upload_to_stage(file_path, stage_name, conn): Uploads a CSV file to a Snowflake stage.
•	load_csv_to_snowflake(file_path, table_name, stage_name): Loads CSV data into Snowflake.
•	process_csv_files(directory, stage_name): Processes all CSV files in a directory and loads them into Snowflake.
Usage Instructions
Token Refresh Script
- Open a command prompt or terminal.
- Navigate to the directory containing the tokenrefresh.py script.
- Run the script:
     python tokenrefresh.py
Data Fetching Script
- Open a command prompt or terminal.
-	Navigate to the directory where the main.py file is located.
-	Run the script:
     python main.py
Snowflake Loading Script
- Place your CSV files in the directory where the snow_load.py script is located.
- Open a command prompt or terminal and navigate to that directory.
- Run the script:
     python snow_load.py
Error Handling and Troubleshooting
If you encounter any issues:
•	Verify the environment variables are correctly set in the Windows registry.
•	Ensure the script is run with administrative privileges.
•	Check for error messages in the console for more information.
Contributing
Contributions are welcome! If you find any issues or have suggestions for improvements, please open an issue or submit a pull request.
License
This project is licensed under the MIT License.

