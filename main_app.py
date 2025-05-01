import os

import pandas as pd
import streamlit as st

st.set_page_config(layout="wide")

from adls_utils import (
    convert_parquet_to_excel,
    download_file,
    handle_buttons,
    initialize_storage_account,
    list_files_in_directory,
    load_credentials,
)
from helper_functions import (
    get_allowed_table_names,
    get_yaml_definitions,
    log_event,
    preview_file,
    upload_file_to_blob,
    validate_file,
    version,
)

try:
    # Get YAML table definitions
    _, _, table_names_and_alias = get_yaml_definitions()

    available_allowed_table_names_dict = get_allowed_table_names(table_names_and_alias)

    # Base path for templates
    base_templates_path = "templates/"

    # Load credentials and initialize the Azure Data Lake Storage client
    config = load_credentials()
    service_client = initialize_storage_account(
        config["tenant_id"],
        config["client_id"],
        config["client_secret"],
        config["account_name"],
    )

except Exception as e:
    st.error(f"An unexpected error occurred before main: {e}")


# Main function to render the Streamlit app
def main():
    try:
        try:
            st.title("File Uploader")
            st.caption(
                f"Current version of the application: {version()}"
            )  # Display the current version on the application

            upload_tab, preview_tab, validate_tab, submit_tab, explore_tab = st.tabs(
                ["Upload", "Preview", "Validate", "Submit", "Explorer"]
            )

        except Exception as e:
            st.error(f"An unexpected error occurred before tabs: {e}")

        with upload_tab:
            try:
                if available_allowed_table_names_dict:
                    select_report_type_list = list(
                        available_allowed_table_names_dict.values()
                    )
                    select_report_type_list.sort()

                    # Set selectbox with user friendly alias
                    selected_report_type = st.selectbox(
                        "Select a report type:", select_report_type_list
                    )

                    # Get report name based on alias.
                    report_type = available_allowed_table_names_dict.inverse[
                        selected_report_type
                    ]
                    st.session_state["report_type"] = report_type
                    uploaded_file = st.file_uploader(
                        "Upload your Excel or CSV file", type=["csv", "xlsx"]
                    )

                    if uploaded_file:
                        log_event(f"Report type selected: {report_type}")
                        st.session_state["uploaded_file"] = uploaded_file
                        st.session_state["file_type"] = (
                            ".csv" if uploaded_file.name.endswith(".csv") else ".xlsx"
                        )  # Set the 'file_type' in the session state based on the file extension of the uploaded file;

                        # Remove the temporary file if it exists
                        if "temp_file_path" in st.session_state and os.path.exists(
                            st.session_state["temp_file_path"]
                        ):
                            os.remove(st.session_state["temp_file_path"])

                    st.markdown("---")
                    st.caption("Download templates")

                    # Assume templates are named based on the report type and stored in the 'templates' directory
                    excel_template_path = os.path.join(
                        base_templates_path, f"{report_type}.xlsx"
                    )
                    csv_template_path = os.path.join(
                        base_templates_path, f"{report_type}.csv"
                    )

                    # Download buttons for templates
                    try:
                        with open(excel_template_path, "rb") as excel_file:
                            st.download_button(
                                label="Excel template",
                                data=excel_file,
                                file_name=f"{report_type}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            )
                    except FileNotFoundError:
                        st.error(f"Excel template for {report_type} not found.")
                        st.write(excel_template_path)

                    try:
                        with open(csv_template_path, "rb") as csv_file:
                            st.download_button(
                                label="CSV template  \u00a0",
                                data=csv_file,
                                file_name=f"{report_type}.csv",
                                mime="text/csv",
                            )
                    except FileNotFoundError:
                        st.error(f"CSV template for {report_type} not found.")

            except Exception as e:
                st.error(f"An unexpected error occurred in upload_tab: {e}")

        with preview_tab:
            try:
                if "uploaded_file" in st.session_state:
                    uploaded_file = st.session_state["uploaded_file"]

                    df_prop_filtered, read_auto_table = preview_file(uploaded_file)
                    st.session_state["read_auto_table"] = read_auto_table

                    with st.expander("File content:", expanded=True):
                        st.data_editor(
                            read_auto_table,
                            disabled=True,
                            use_container_width=True,
                            key="unique_data_editor_read_auto_table_preview",
                        )

                    # Create 2 columns in Streamlit with different widths
                    col1, _ = st.columns([0.7, 1])  # Setting the width of the columns

                    # Show CSV detected properties only if file type is CSV
                    if st.session_state["file_type"] == ".csv":
                        with col1:
                            with st.expander("Detected properties", expanded=False):
                                # st.dataframe(df_prop_filtered, use_container_width=True)
                                st.data_editor(
                                    df_prop_filtered,
                                    disabled=True,
                                    use_container_width=True,
                                    key="unique_data_editor_df_prop_filtered",
                                )

                else:
                    st.warning("Please upload the file before preview.")

            except Exception as e:
                st.error(f"An unexpected error occurred in validate_tab: {e}")

        with validate_tab:
            try:
                # Check if the file has been uploaded
                if "uploaded_file" not in st.session_state:
                    st.warning("Please upload the file before validation.")
                else:
                    # validate_file(df_read_auto, read_auto_table)
                    validate_file(read_auto_table)

            except Exception as e:
                st.error(f"An unexpected error occurred in validate_tab: {e}")

        with submit_tab:
            try:
                # if 'casted_df_read_auto' in st.session_state and st.session_state.get('column_is_valid') and st.session_state.get('dbt_tests_passed'):
                #     processed_csv = st.session_state['casted_df_read_auto'].to_csv(index=False).encode('utf-8')

                if (
                    "casted_read_auto_table" in st.session_state
                    and st.session_state.get("column_is_valid")
                    and st.session_state.get("dbt_tests_passed")
                ):
                    # processed_csv = st.session_state['casted_read_auto_table'].to_csv(index=False).encode('utf-8')
                    # st.download_button("Download Processed CSV", processed_csv, f"{st.session_state['report_type']}.csv", "text/csv", key='download-csv')

                    log_event("Processed file available for download")

                    if st.button("Submit"):
                        ##############################################################
                        with st.spinner("Uploading file to Blob Storage..."):
                            # upload_status = upload_file_to_blob(st.session_state['casted_df_read_auto'], st.session_state['report_type'], st.session_state['uploaded_file'].name)
                            upload_status = upload_file_to_blob(
                                st.session_state["casted_read_auto_table"],
                                st.session_state["report_type"],
                                st.session_state["uploaded_file"].name,
                            )
                            st.success(upload_status[0])
                            st.info(upload_status[1] + "\n\n" + upload_status[2])
                            log_event("File stored into ADLS")
                        ##############################################################

                    os.remove(st.session_state["temp_file_path"])

                else:
                    st.warning("Please validate the file before submitting.")

            except Exception as e:
                st.error(f"An unexpected error occurred in submit_tab: {e}")

        # Tab 5: Browse, Preview, Delete and Download Files
        with explore_tab:
            try:
                # st.caption("Browse, Preview, Delete and Download Files")

                st.caption("""*Only one file can be Previewed, Converted to Excel or Downloaded at a time. 
                                One or multiple files can be Deleted simultaneously.""")

                # Ensure the report type is selected
                if report_type:
                    full_path = f"{config['base_path']}/{report_type}"

                    # Arrange buttons in a row
                    col1, col2, col3, col4 = st.columns([1, 1, 1, 7], gap="medium")
                    with col1:
                        preview_button_clicked = st.button(
                            "Preview", use_container_width=True
                        )
                    with col2:
                        delete_button_clicked = st.button(
                            "Delete", use_container_width=True
                        )
                    with col3:
                        convert_button_clicked = st.button(
                            "Convert to Excel", use_container_width=True
                        )
                    with col4:
                        # Check if there is a flag to show the download button
                        if st.session_state.get("show_download_excel", False):
                            try:
                                # Convert the selected Parquet file to Excel format
                                excel_data = convert_parquet_to_excel(
                                    download_file(
                                        service_client,
                                        config["container_name"],
                                        st.session_state.selected_file,
                                    )
                                )
                                # Provide the download button if the conversion is successful
                                if excel_data:
                                    st.download_button(
                                        label="Download Excel File",
                                        data=excel_data,
                                        file_name=f"{st.session_state.selected_file}.xlsx",
                                    )
                                # Reset session state flags after download
                                st.session_state.show_download_excel = False
                                st.session_state.selected_file = None
                            except Exception as e:
                                st.error(f"Error converting or downloading file: {e}")

                    # List files in the selected directory
                    file_list = list_files_in_directory(
                        service_client, config["container_name"], full_path
                    )
                    if file_list:
                        # Create a DataFrame for the file list with a checkbox for deletion
                        df = pd.DataFrame(file_list, columns=["File Name"])
                        df["Select"] = False

                        # Display the DataFrame as an editable table
                        edited_df = st.data_editor(
                            df,
                            use_container_width=True,
                            hide_index=True,
                            num_rows="fixed",
                            column_config={
                                "Select": st.column_config.CheckboxColumn("Select")
                            },
                        )
                        # Get the list of files selected for deletion
                        selected_files = edited_df[edited_df["Select"]][
                            "File Name"
                        ].tolist()

                        # Handle button actions (preview, delete, convert)
                        handle_buttons(
                            service_client,
                            config["container_name"],
                            selected_files,
                            preview_button_clicked,
                            delete_button_clicked,
                            convert_button_clicked,
                        )
                    else:
                        # Message if no files are found in the directory
                        st.write("No files found in this directory.")
            except Exception as e:
                st.error(f"An unexpected error occurred in explore_tab: {e}")

    except Exception as e:
        st.error(f"An unexpected error occurred in main: {e}")


if __name__ == "__main__":
    main()
