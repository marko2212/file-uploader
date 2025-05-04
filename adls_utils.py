import os
import time
from io import BytesIO

import pandas as pd
import pyarrow.parquet as pq
import streamlit as st
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient


def load_credentials():
    """
    Load credentials from environment variables.

    Returns:
        dict: A dictionary containing the credentials.
    """
    return {
        "tenant_id": os.getenv("AZURE_TENANT_ID"),
        "client_id": os.getenv("AZURE_CLIENT_ID"),
        "client_secret": os.getenv("AZURE_CLIENT_SECRET"),
        "account_name": os.getenv("AZURE_STORAGE_ACCOUNT_NAME"),
        "container_name": os.getenv("AZURE_STORAGE_CONTAINER_NAME"),
        "base_path": os.getenv("AZURE_STORAGE_FILE_PATH"),
    }


def initialize_storage_account(tenant_id, client_id, client_secret, account_name):
    """
    Initialize the Azure Data Lake Storage account.

    Args:
        tenant_id (str): The tenant ID for the Azure account.
        client_id (str): The client ID for the Azure account.
        client_secret (str): The client secret for the Azure account.
        account_name (str): The storage account name.

    Returns:
        DataLakeServiceClient: The initialized DataLakeServiceClient object.
    """
    try:
        # Create a credential object using client secret
        credential = ClientSecretCredential(tenant_id, client_id, client_secret)
        # Initialize the DataLakeServiceClient with the provided account URL and credentials
        service_client = DataLakeServiceClient(
            account_url=f"https://{account_name}.dfs.core.windows.net",
            credential=credential,
        )
        return service_client
    except Exception as e:
        # Handle connection errors
        st.error(f"Failed to connect to ADLS: {e}")
        return None


def list_files_in_directory(service_client, file_system_name, directory_name):
    """
    List files in a specified directory in Azure Data Lake Storage.

    Args:
        service_client (DataLakeServiceClient): The service client for the Azure Data Lake Storage account.
        file_system_name (str): The name of the file system.
        directory_name (str): The name of the directory.

    Returns:
        list: A list of file names in the directory.
    """
    try:
        # Get the file system client
        file_system_client = service_client.get_file_system_client(file_system_name)
        # List all paths (files and directories) in the specified directory
        paths = file_system_client.get_paths(path=directory_name)
        return [path.name for path in paths]
    except Exception as e:
        # Handle errors during file listing
        st.error(f"Failed to list files: {e}")
        return []


def download_file(service_client, file_system_name, selected_file):
    """
    Download a file from Azure Data Lake Storage.

    Args:
        service_client (DataLakeServiceClient): The service client for the Azure Data Lake Storage account.
        file_system_name (str): The name of the file system.
        selected_file (str): The path of the file to download.

    Returns:
        bytes: The contents of the downloaded file.
    """
    try:
        # Get the file system client
        file_system_client = service_client.get_file_system_client(file_system_name)
        # Get the file client for the specified file
        file_client = file_system_client.get_file_client(selected_file)
        # Download the file
        download = file_client.download_file()
        return download.readall()
    except Exception as e:
        # Handle errors during file download
        st.error(f"Failed to download file: {e}")
        return None


def preview_parquet(data):
    """
    Preview the first 50 rows of a Parquet file.

    Args:
        data (bytes): The Parquet file data.

    Returns:
        pd.DataFrame: The first 50 rows of the Parquet file as a DataFrame.
    """
    try:
        # Load data into a buffer
        buffer = BytesIO(data)
        # Read the Parquet table from the buffer
        table = pq.read_table(buffer)
        # Convert the table to a pandas DataFrame and return the first 50 rows
        return table.to_pandas().head(50)
    except Exception as e:
        # Handle errors during preview
        st.error(f"Failed to preview Parquet file: {e}")
        return None


def convert_parquet_to_excel(parquet_data):
    """
    Convert Parquet file data to an Excel file.

    Args:
        parquet_data (bytes): The Parquet file data.

    Returns:
        bytes: The contents of the Excel file.
    """
    try:
        # Load data into a buffer
        buffer = BytesIO(parquet_data)
        # Read the Parquet table from the buffer
        table = pq.read_table(buffer)
        # Convert the table to a pandas DataFrame
        df = table.to_pandas()
        # Columns to drop from the DataFrame
        columns_to_drop = [
            "deltalake_loadtime",
            "deltalake_filename",
            "original_filename",
        ]
        # Drop specified columns if they exist
        df.drop(
            columns=[col for col in columns_to_drop if col in df.columns],
            axis=1,
            inplace=True,
        )
        # Create an Excel buffer
        excel_buffer = BytesIO()
        # Write the DataFrame to an Excel file
        with pd.ExcelWriter(excel_buffer, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Sheet1")
        return excel_buffer.getvalue()
    except Exception as e:
        # Handle errors during conversion
        st.error(f"Failed to convert Parquet to Excel: {e}")
        return None


def delete_files(service_client, file_system_name, selected_files):
    """
    Delete specified files from Azure Data Lake Storage.

    Args:
        service_client (DataLakeServiceClient): The service client for the Azure Data Lake Storage account.
        file_system_name (str): The name of the file system.
        selected_files (list): A list of file paths to delete.
    """
    try:
        # Get the file system client
        file_system_client = service_client.get_file_system_client(file_system_name)
        for selected_file in selected_files:
            # Get the file client for each file
            file_client = file_system_client.get_file_client(selected_file)
            # Delete the file
            file_client.delete_file()
        st.success(f"Files {', '.join(selected_files)} deleted successfully!")
    except Exception as e:
        # Handle errors during deletion
        st.error(f"Failed to delete files: {e}")


@st.dialog("Delete Confirmation")
def confirm_delete(service_client, container_name, selected_files):
    """
    Display a confirmation dialog for deleting files.

    Args:
        service_client (DataLakeServiceClient): The service client for the Azure Data Lake Storage account.
        container_name (str): The name of the container.
        selected_files (list): A list of file paths to delete.
    """
    # Displays a message asking the user to confirm the deletion of the specified files.
    st.write(
        f"Are you sure you want to delete the following files:\n\n{', '.join(selected_files)}?"
    )

    # If the user presses the "Yes, delete" button, call the function to delete files.
    if st.button("Yes, delete", key="confirm_yes"):
        delete_files(service_client, container_name, selected_files)
        time.sleep(2)  # Short pause before refreshing the application.
        st.rerun()  # Reruns the application to refresh the display.

    # If the user presses the "No, cancel" button, display a cancellation message.
    if st.button("No, cancel", key="confirm_no"):
        st.error("Deletion canceled.")
        time.sleep(2)  # Short pause before refreshing the application.
        st.rerun()  # Reruns the application to refresh the display.


import streamlit as st


def handle_buttons(
    service_client,
    container_name,
    selected_files,
    preview_button_clicked,
    delete_button_clicked,
    convert_button_clicked,
):
    """
    Handle actions for the preview, delete, and convert buttons.

    Args:
        service_client (DataLakeServiceClient): The service client for the Azure Data Lake Storage account.
        container_name (str): The name of the container.
        selected_files (list): A list of selected files.
        preview_button_clicked (bool): Flag indicating if the preview button was clicked.
        delete_button_clicked (bool): Flag indicating if the delete button was clicked.
        convert_button_clicked (bool): Flag indicating if the convert button was clicked.
    """
    try:
        # If the delete button is clicked, proceed with the delete action.
        if delete_button_clicked:
            if selected_files:
                confirm_delete(service_client, container_name, selected_files)
            else:
                st.warning("Please select at least one file to delete.")

        # If the preview button is clicked, proceed with the preview action.
        if preview_button_clicked:
            if len(selected_files) == 1:
                try:
                    file_data = download_file(
                        service_client, container_name, selected_files[0]
                    )
                except Exception as e:
                    st.error(f"Error downloading file: {e}")
                    return

                if file_data:
                    try:
                        preview_df = preview_parquet(file_data)
                    except Exception as e:
                        st.error(f"Error previewing file: {e}")
                        return

                    if preview_df is not None:
                        with st.expander(
                            f"Preview file (the first 50 rows): \n\n{selected_files[0]}",
                            expanded=True,
                        ):
                            st.data_editor(preview_df, disabled=True)
                    else:
                        st.error("Failed to preview the file.")
            elif len(selected_files) > 1:
                st.warning("Please select only one file to preview.")
            else:
                st.warning("Please select a file to preview.")

        # If the convert button is clicked, proceed with the convert and download action.
        if convert_button_clicked:
            if len(selected_files) == 1:
                st.session_state.selected_file = selected_files[0]
                st.session_state.show_download_excel = True
                st.rerun()
            elif len(selected_files) > 1:
                st.warning("Please select only one file to convert and download.")
            else:
                st.warning("Please select a file to convert and download.")
    except Exception as e:
        st.error(f"An unexpected error occurred: {e}")
