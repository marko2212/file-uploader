import datetime
import json
import logging
import os
import re
import shutil
import tempfile
from io import BytesIO

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import streamlit as st
import yaml
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from bidict import bidict
from dbt.cli.main import dbtRunner, dbtRunnerResult
from dotenv import dotenv_values, load_dotenv

# Setting up logging
logging.basicConfig(
    filename="app.log", level=logging.INFO, format="%(asctime)s - %(message)s"
)


def log_event(message):
    logging.info(message)


# Function to return the current version of the application
def version():
    return "1.13.0"  # Increment this value whenever a new version is released


# Load environment variables from .env file
if os.path.exists(".env"):
    load_dotenv(override=True)
    config = dotenv_values(".env")


def get_pyarrow_dtype(dtype):
    """
    Map a YAML string data type descriptor to a corresponding PyArrow data type.

    Args:
    dtype (str): A string representing the data type, e.g., 'string', 'decimal(10,2)', 'date'.

    Returns:
    pyarrow.DataType: The corresponding PyArrow data type.

    Raises:
    ValueError: If the provided data type is not supported.
    """
    if dtype == "string":
        return pa.string()
    elif dtype.startswith("decimal"):
        # Extract precision and scale for decimal types
        precision, scale = map(
            int, dtype[dtype.find("(") + 1 : dtype.find(")")].split(",")
        )
        return pa.decimal128(precision, scale)
    elif dtype == "date":
        return pa.date32()
    elif dtype == "int32":
        return pa.int32()
    elif dtype == "int64":
        return pa.int64()
    elif dtype == "float32":
        return pa.float32()
    elif dtype == "float64":
        return pa.float64()
    elif dtype == "bool":
        return pa.bool_()
    # Add additional data types as necessary
    else:
        raise ValueError(f"Unsupported data type: {dtype}")


def cast_pyarrow_table_columns_to_types(table, model_data_types):
    """
    Casts columns of a PyArrow Table to specified data types based on a schema.

    Args:
        table (pyarrow.Table): The input table to be converted.
        model_data_types (dict): A dictionary mapping column names to data type descriptors.

    Returns:
        pyarrow.Table: The table with columns cast to the specified data types.
    """
    new_columns = []
    all_column_type_matched = True

    for column_name in table.schema.names:
        if column_name in model_data_types:
            try:
                dtype = model_data_types[column_name]
                arrow_type = get_pyarrow_dtype(dtype)

                # pyarrow.csv or duckdb int values such as [1, 2, 3] will be casted to int64
                # due to pyarrow cannot cast directly from int64 to decimal(16,4)
                # Check if the column is of int64 type and needs to be cast to decimal(16,4)
                if (
                    table.column(column_name).type == pa.int64()
                    and dtype == "decimal(16,4)"
                ):
                    # Step 1: Cast from int64 to int32
                    intermediate_column = table.column(column_name).cast(pa.int32())
                    # Step 2: Cast from int32 to decimal(16,4)
                    casted_column = intermediate_column.cast(arrow_type)
                else:
                    # Direct casting for other types
                    casted_column = table.column(column_name).cast(arrow_type)

                new_columns.append(casted_column)
                print(f"Column '{column_name}' successfully cast to {dtype}.")
            except Exception as e:
                # Attempt to show unique values from the problematic column
                try:
                    unique_values = pc.unique(table.column(column_name))
                    if len(unique_values) > 5:
                        unique_values = unique_values.slice(0, 5)
                    examples = unique_values.to_pandas().tolist()
                except Exception as e_inner:
                    examples = ["Error fetching examples: " + str(e_inner)]

                error_message = f"""
                    Error converting column '{column_name}' to `{dtype}`.
                    - Error Details: `{str(e)}`
                    - Examples of data from the column: {examples}
                    """
                print(error_message)
                st.warning(error_message)
                new_columns.append(table.column(column_name))
                all_column_type_matched = False
        else:
            # If the column is not in the schema, use the original column
            new_columns.append(table.column(column_name))

    if all_column_type_matched:
        print("All columns have been successfully cast.")
    else:
        print("Not all columns could be converted to the specified types.")

    return pa.Table.from_arrays(
        new_columns, names=table.schema.names
    ), all_column_type_matched


def save_uploaded_file(uploaded_file):
    """
    Saves uploaded file to a temporary file and returns the path.

    Args:
        uploaded_file: The uploaded file to save.

    Returns:
        str: The file path where the uploaded file is saved.
    """

    if uploaded_file is None:
        st.error("No file uploaded.")
        return None

    # Determine file type based on the file extension or from session state
    file_type = st.session_state["file_type"]

    try:
        # Create a temporary file with the appropriate file extension
        with tempfile.NamedTemporaryFile(
            delete=False, suffix=file_type, mode="wb"
        ) as temp_file:
            # Write the content of the uploaded file to the temporary file
            shutil.copyfileobj(uploaded_file, temp_file)
            temp_file_path = temp_file.name

        # Optional: Show the path in the Streamlit app (for debugging)
        # st.write(f"Temporary file saved to {temp_file_path}")

        return temp_file_path

    except Exception as e:
        st.error(f"Failed to save uploaded file: {e}")
        return None


# Function to read schema.yml files and get table column definitions
def get_yaml_definitions():
    # Current working directory
    base_dir = os.getcwd()
    # Relative path from the current working directory
    yaml_relative_path = os.path.join(
        base_dir, "FileUploaderDBT", "models", "validation"
    )
    configuration_file_paths = []
    for root, _, files in os.walk(yaml_relative_path):
        for file in files:
            if file.endswith(".yml"):
                configuration_file_paths.append(os.path.join(root, file))

    report_name_and_alias_dict = bidict()
    columns_by_table = {}
    columns_type_by_table = {}

    for path in configuration_file_paths:
        try:
            # For each model definition
            with open(path, "r") as file:
                content = yaml.safe_load(file)

                # Get the first defined source
                model = content.get("sources", [])[0]

                # Get the first defined table
                table = model.get("tables", [])[0]

                # Get Report name
                report_name = table.get("name")
                report_name_and_alias_dict[report_name] = report_name

                # Get report alias if exists.
                table_alias = table.get("table_alias")
                if table_alias:
                    report_name_and_alias_dict[report_name] = table_alias

                # Placeholder for column_names & column_datatypes
                column_name_dict = {}
                column_datatype_dict = {}
                # Get column information
                for col in table.get("columns", []):
                    # Column names
                    sanatized_name = sanatize_string(col["name"])
                    column_name_dict[sanatized_name] = col["name"]

                    # datatypes
                    column_datatype_dict[col["name"]] = col["data_type"]

                columns_by_table[report_name] = column_name_dict
                columns_type_by_table[report_name] = column_datatype_dict

        except yaml.YAMLError as exc:
            logging.error(f"Error reading YAML file {path}: {exc}")
        except FileNotFoundError:
            logging.error(f"File not found: {path}")

    return columns_by_table, columns_type_by_table, report_name_and_alias_dict


def read_csv_and_excel_files(temp_file_path, file_type):
    """
    Reads and processes CSV or Excel files, detecting properties and types based on file content using DuckDB.

    Args:
        temp_file_path (str): Path to the file to be processed.
        file_type (str): Type of the file ('.csv' or '.xlsx').

    Returns:
        tuple: Depending on the file type, returns a tuple:
            - For CSV: DataFrame of properties, DataFrame from READ_CSV_AUTO, Arrow Table.
            - For Excel: DataFrame (empty for properties), DataFrame from st_read, Arrow Table.
    """

    file_type = st.session_state["file_type"]

    # Connect to the DuckDB instance
    con = duckdb.connect(database=":memory:", read_only=False)

    # Install and load the spatial extension
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")

    # Try to read the file based on its type and catch any exceptions
    try:
        if file_type == ".csv":
            # Processing CSV files
            try:
                # Use DuckDB's sniff_csv to get properties
                df_csv_prop_sniff = con.execute(
                    f"SELECT * FROM sniff_csv('{temp_file_path}')"
                ).fetchdf()
                read_auto_table = con.execute(
                    f"SELECT * FROM read_csv_auto('{temp_file_path}')"
                ).fetch_arrow_table()

                # Remove empty rows
                read_auto_table = remove_empty_rows(read_auto_table)

                # Strip column names for special characters.
                read_auto_table = sanatize_table_column_names(read_auto_table)

                # Transpose the sniffed properties DataFrame and filter
                df_csv_prop_transposed = df_csv_prop_sniff.T
                df_csv_prop_transposed.reset_index(inplace=True)
                df_csv_prop_transposed.columns = ["Properties", "Values"]
                df_csv_prop_filtered = df_csv_prop_transposed[
                    ~df_csv_prop_transposed["Properties"].isin(["Columns", "Prompt"])
                ]  # Filter out 'Columns' and 'Prompt' rows

                return df_csv_prop_filtered, read_auto_table

            except Exception as e:
                st.error(f"Error processing CSV file: {e}")
                return pd.DataFrame(), pa.Table.from_pandas(pd.DataFrame())

        elif file_type == ".xlsx":
            # Processing Excel files using DuckDB
            try:
                read_auto_table = con.execute(
                    f"SELECT * FROM st_read('{temp_file_path}', open_options = ['HEADERS=FORCE'])"
                ).fetch_arrow_table()

                # Remove empty rows
                read_auto_table = remove_empty_rows(read_auto_table)

                # Strip column names for special characters.
                read_auto_table = sanatize_table_column_names(read_auto_table)

                return pd.DataFrame(), read_auto_table

            except Exception as e:
                st.error(f"Error processing Excel file: {e}")
                return pd.DataFrame(), pa.Table.from_pandas(pd.DataFrame())

        else:
            raise ValueError("Unsupported file type provided. Use 'csv' or 'xlsx'.")

    except Exception as e:
        st.error(f"An error occurred: {e}")
        return pd.DataFrame(), pa.Table.from_pandas(pd.DataFrame())

    finally:
        # Ensure the connection is closed after processing
        con.close()


def preview_file(
    uploaded_file,
):  # Created for Preview tab. Preview uploaded data as is.
    """
    Preview the uploaded file and detect its properties using the read_csv_and_excel_files function.

    Args:
        uploaded_file: The uploaded file to be processed.

    Returns:
        tuple: Depending on the file type, returns a tuple:
            - DataFrame of properties,
            - DataFrame from reading the file,
            - Arrow Table from reading the file.
    """
    file_type = st.session_state["file_type"]

    # Ensure file_type is normalized to be without the initial dot
    file_type = file_type.lower().replace(".", "")

    # Check if the uploaded file is not None
    if uploaded_file is None:
        st.error("No file uploaded.")
        return None, None

    try:
        # Save the uploaded file and update the session state
        temp_file_path = save_uploaded_file(uploaded_file)
        st.session_state["temp_file_path"] = temp_file_path

        # Use the read_csv_and_excel_files function to process the file
        df_prop_filtered, read_auto_table = read_csv_and_excel_files(
            temp_file_path, file_type
        )
        return df_prop_filtered, read_auto_table

    except ValueError as ve:
        st.error(f"Unsupported file type: {ve}")
        return None, None

    except Exception as e:
        st.error(f"Error loading file: {e}")
        return None, None


def validate_column_names(arrow_table, yaml_columns):
    """
    Validates column names in the uploaded file against the expected columns defined in the YAML configuration.

    Args:
        arrow_table (pa.Table): The Arrow Table containing the data from the uploaded file.
        yaml_columns (dict): Get expected columns for the report from YAML configuration

    Returns:
        bool: True if the uploaded file's columns match the expected columns, False otherwise.
    """
    # Convert column names to lowercase for comparison
    file_columns = [col.lower() for col in arrow_table.schema.names]

    # Strip special characters
    file_columns = sanatize_string_list(file_columns)

    # Find missing and extra columns
    missing_in_file = set(yaml_columns) - set(file_columns)
    extra_in_file = set(file_columns) - set(yaml_columns)

    # Validate column names against YAML configuration
    if missing_in_file or extra_in_file:
        if missing_in_file:
            st.error(f"Columns in the YAML but not in the file: {missing_in_file}")
        if extra_in_file:
            st.error(f"Columns in the file but not in the YAML: {extra_in_file}")
        return False
    else:
        st.success("Column names in the file match the YAML file.")
        return True


# Later in your code, use this context manager
def run_dbt(table_name):
    # Initialize DBT runner

    os.chdir("FileUploaderDBT")

    dbt = dbtRunner()

    # Create CLI arguments as a list of strings
    cli_args = ["test", "--select", f"source:uploaded_files.{table_name}"]

    # Run the DBT command
    try:
        res: dbtRunnerResult = dbt.invoke(cli_args)

        # Check and display the results
        if res.result:
            for r in res.result:
                print(f"{r.node.name}: {r.status}")
                # st.write(f"{r.node.name}: {r.status}")
        else:
            print("No result from DBT run")
    except Exception as e:
        print(f"Error during DBT run: {e}")
        st.error(f"Error during DBT run: {e}")

    os.chdir("..")


# Creating columns for width control
def display_test_summary(summary_df):
    col1, _, _ = st.columns([3, 3, 3])  # Adjusting the width of the columns
    # Display the summary of DBT test results using Streamlit
    with col1:
        with st.expander("Summary", expanded=True):
            st.dataframe(summary_df, use_container_width=True)


# Processes failed DBT tests and returns a DataFrame with their details.
def process_failed_tests(failed_tests):
    for test in failed_tests:
        test["unique_id"] = test["unique_id"].split(".")[
            -2
        ]  # Trim the unique_id to display a more readable format

    # Create a DataFrame for detailed information of failed tests
    failed_tests_df = pd.DataFrame(failed_tests)
    failed_tests_df = failed_tests_df[
        ["failures", "unique_id", "compiled_code", "message"]
    ]
    failed_tests_df.columns = [
        "Number of failures",
        "Test name",
        "Compiled SQL",
        "Message",
    ]
    return failed_tests_df


# Function to process and display the results of DBT tests
def process_dbt_results(results_file):
    # Check if the results file exists
    if os.path.exists(results_file):
        # Load the results file
        with open(results_file) as f:
            run_results = json.load(f)

        # Calculate the total number of tests and failed tests
        total_tests = len(run_results["results"])
        failed_tests = [
            result for result in run_results["results"] if result["status"] == "fail"
        ]
        passed_tests_count = total_tests - len(failed_tests)

        # Create a summary DataFrame to display overall test results
        summary_df = pd.DataFrame(
            {
                "Status": ["Passed", "Failed"],
                "Count": [passed_tests_count, len(failed_tests)],
            }
        )

        # Process and display details of failed tests if any
        if failed_tests:
            # # Display detailed information about failed tests using Streamlit
            st.error(f"Found {len(failed_tests)} failed tests.")

            # Display the summary of DBT test results in an expander
            display_test_summary(summary_df)

            # Process the failed tests
            failed_tests_df = process_failed_tests(failed_tests)

            with st.expander("Details", expanded=True):
                st.dataframe(failed_tests_df, use_container_width=True)

            st.session_state["dbt_tests_passed"] = False

            return False

        else:
            # Display a success message if all tests passed
            st.success("All DBT tests passed successfully!")

            # Display the summary of DBT test results in an expander
            display_test_summary(summary_df)

            st.session_state["dbt_tests_passed"] = True

            return True
    else:
        # Display an error message if the results file is not found
        st.error("DBT test results file not found.")

        return False


def validate_file(read_auto_table):
    report_type = st.session_state["report_type"]

    # Get YAML table definitions
    columns_by_table, columns_type_by_table, _ = get_yaml_definitions()

    # Column names validation
    column_is_valid = validate_column_names(
        read_auto_table, columns_by_table[report_type]
    )
    st.session_state["column_is_valid"] = column_is_valid

    # Rename columns to follow schema
    read_auto_table = read_auto_table.rename_columns(columns_by_table[report_type])

    # # Column types validation
    if column_is_valid:
        casted_read_auto_table, all_column_type_matched = (
            cast_pyarrow_table_columns_to_types(
                read_auto_table, columns_type_by_table[report_type]
            )
        )

        st.session_state["all_column_type_matched"] = all_column_type_matched

        # Display casted_read_auto_table
        with st.expander("File content", expanded=True):
            st.data_editor(
                casted_read_auto_table,
                disabled=True,
                use_container_width=True,
                key="unique_data_editor_read_auto_table_validation",
            )

        if all_column_type_matched:
            # Insert casted_read_auto_table into duckdb for futrther dbt testing
            con = duckdb.connect(database="db.duckdb", read_only=False)
            con.execute(
                f"CREATE OR REPLACE TABLE {report_type} AS SELECT * FROM casted_read_auto_table"
            )
            con.close()

            ##############################################################
            # Run DBT tests after inserting in DuckDB
            with st.spinner(
                f"Running DBT tests for report {report_type}..."
            ):  # Display a spinner while the function is running
                run_dbt(report_type)
                ##############################################################
                # Horizontal line for visual separation of sections
                st.markdown("---")
                st.subheader("DBT tests")

                # Define the path to the DBT results file and process the results
                results_file = os.path.join(
                    "FileUploaderDBT", "target", "run_results.json"
                )
                all_tests_passed = process_dbt_results(results_file)
                ##############################################################

                st.session_state["all_tests_passed"] = all_tests_passed
                st.session_state["casted_read_auto_table"] = casted_read_auto_table

                log_event("File processed and displayed")

    else:
        if "casted_read_auto_table" in st.session_state:
            del st.session_state["casted_read_auto_table"]


def upload_file_to_blob(casted_read_auto_table, report_type, uploaded_file_name):
    try:
        # Load credentials from environment variables
        tenant_id = os.getenv("AZURE_TENANT_ID")
        client_id = os.getenv("AZURE_CLIENT_ID")
        client_secret = os.getenv("AZURE_CLIENT_SECRET")

        storage_account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
        container_name = os.getenv(
            "AZURE_STORAGE_CONTAINER_NAME"
        )  # The blob name to be used for the file upload
        blob_path = os.getenv("AZURE_STORAGE_FILE_PATH")

        # Create Azure AD credentials
        credential = ClientSecretCredential(
            tenant_id=tenant_id, client_id=client_id, client_secret=client_secret
        )

        # Create a Blob Storage service client using the service principal for authentication
        blob_service_client = BlobServiceClient(
            account_url=f"https://{storage_account_name}.blob.core.windows.net",
            credential=credential,
        )

        # Current time for metadata
        deltalake_loadtime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Generate new file name
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        new_filename = f"{uploaded_file_name.split('.')[0]}_{timestamp}.parquet"  # Remove extension and add timestamp

        # Generate blob name
        blob_name = f"{blob_path}/{report_type}/{new_filename}"
        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=blob_name
        )

        # Create new columns
        loadtime_array = pa.array(
            [deltalake_loadtime] * len(casted_read_auto_table), pa.string()
        )
        filename_array = pa.array(
            [uploaded_file_name] * len(casted_read_auto_table), pa.string()
        )
        deltalake_filename_array = pa.array(
            [new_filename] * len(casted_read_auto_table), pa.string()
        )

        # Add columns to the pyarrow table
        casted_read_auto_table = casted_read_auto_table.append_column(
            "deltalake_loadtime", loadtime_array
        )
        casted_read_auto_table = casted_read_auto_table.append_column(
            "original_filename", filename_array
        )
        casted_read_auto_table = casted_read_auto_table.append_column(
            "deltalake_filename", deltalake_filename_array
        )

        # Convert the PyArrow Table to Parquet format and write directly to Blob Storage
        buffer = BytesIO()
        pq.write_table(casted_read_auto_table, buffer)
        buffer.seek(0)  # Reset the buffer position to the beginning
        blob_client.upload_blob(buffer.getvalue(), overwrite=True)
        ######################################

        return (
            f"File '{new_filename}' successfully uploaded to Blob Storage.",
            f"Original uploaded file name: '{uploaded_file_name}",
            f"Report type: {report_type}",
        )

    except Exception as e:
        return f"Error uploading file to Blob Storage: {e}"


def sanatize_string(string):
    # Remove special characters using regular expression
    sanitized_string = re.sub(r"[^a-zA-Z0-9 ]", "", string)
    # Convert to lowercase
    sanitized_string = sanitized_string.lower()
    return sanitized_string


def sanatize_string_list(string_list):
    sanitized_list = []
    for string in string_list:
        sanitized_list.append(sanatize_string(string))
    return sanitized_list


def sanatize_table_column_names(pyarrow_table):
    new_names = sanatize_string_list(pyarrow_table.column_names)
    return pyarrow_table.rename_columns(new_names)


def remove_empty_rows(table):
    if isinstance(table, pd.DataFrame):  # Pandas table
        return table.dropna(how="all").reset_index(drop=True)
    elif isinstance(table, pa.lib.Table):  # Pyarrow table
        df = table.to_pandas().dropna(how="all").reset_index(drop=True)
        return pa.Table.from_pandas(df)
    else:
        raise ValueError(
            "Unsupported table type. Must be a pandas DataFrame or PyArrow Table."
        )


def get_allowed_table_names(table_names_and_alias):
    # Fetch allowed table names from environment variable
    allowed_table_names = os.getenv("ALLOWED_TABLE_NAMES", "").split(",")

    # Remove any leading/trailing whitespace from each table name
    allowed_table_names = [table.strip() for table in allowed_table_names]

    available_allowed_table_names = bidict(
        {
            key: value
            for key, value in table_names_and_alias.items()
            if key in allowed_table_names
        }
    )
    # {key: value for key, value in my_dict.items() if key in other_table_keys}

    return available_allowed_table_names
