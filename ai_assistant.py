import streamlit as st
import google.generativeai as genai
import json
import re
import database as db
import sqlite3
import os
from dotenv import load_dotenv

db.init_db()
# --- Securely load Gemini Pro API Key ---
# load_dotenv()
# gemini_api_key = os.getenv("GEMINI_API_KEY")

# if not gemini_api_key:
#     st.error("Gemini Pro API key not found. Please set it in your .env file.")
#     st.stop()
# else:
#     genai.configure(api_key=gemini_api_key)
#     model = genai.GenerativeModel('gemini-2.0-flash')

API_KEY = "AIzaSyA16aeBQI9Lq2xDpMcF5nblYL0x2r8aK5k"
genai.configure(api_key=API_KEY)
model = genai.GenerativeModel(model_name="gemini-2.0-flash")

# System Prompts for different tables and operations
SYSTEM_PROMPTS = {
    "header": """
    You are an expert data analyst and an intelligent conversational assistant for managing data pipeline metadata. Your task is to extract specific field values from a user's plain English input.

    You MUST follow these rules:
    1.  **Extract Data**: Parse the user's input to extract values for the specified fields.
    2.  **Strict Output Format**: You MUST respond with a single JSON object. The JSON object should only contain the keys for the fields you were able to extract from the user's message. Do not include any other text or formatting.
        **Example:** For the input "create pipeline with ID my_pipeline, Business Unit is Finace, ETL Layer is L0", the correct output is `{"DATA_FLOW_GROUP_ID": "my_pipeline", "BUSINESS_UNIT": "Finace", "ETL_LAYER": "L0"}`.
    3.  **No Extraneous Keys**: If a value for a key is not explicitly mentioned or inferred from the user's input, DO NOT include that key in the JSON response.
    4.  **Schema and Rules**: Use the following database schema and rules to guide your extraction and validation.
        -   **data_flow_control_header**:
            -   DATA_FLOW_GROUP_ID (STRING, required)
            -   BUSINESS_UNIT (STRING, required)
            -   BUSINESS_OBJECT_NAME (STRING, required)
            -   TRIGGER_TYPE (STRING, required): Allowed values: DLT, JOB. Dependency: Must be DLT if ETL_LAYER is L0.
            -   ETL_LAYER (STRING, required): Allowed values: L0, L1, L2.
            -   COMPUTE_CLASS (STRING, required)
            -   COMPUTE_CLASS_DEV (STRING, required)
            -   DATA_SME (STRING, required)
            -   PRODUCT_OWNER (STRING, required): Validation: Must be a valid email address.
            -   INGESTION_BUCKET (STRING, required): Allowed values: eda, onedata.
            -   WARNING_THRESHOLD_MINS (INT, required)
            -   WARNING_DL_GROUP (STRING, required)
            -   IS_ACTIVE (STRING, required): Allowed values: Y, N. Default: Y.
            -   INGESTION_MODE (STRING, required for L0)
            -   SPARK_CONFIGS (STRING)
            -   COST_CENTER (STRING)
            -   min_version (DECIMAL)
            -   max_version (DECIMAL)
    """,
    "l0": """
    You are an expert data analyst and an intelligent conversational assistant for managing data pipeline metadata. Your task is to extract specific field values from a user's plain English input for the L0 layer.

    You MUST follow these rules:
    1.  **Extract Data**: Parse the user's input to extract values for the specified fields.
    2.  **Strict Output Format**: You MUST respond with a single JSON object. The JSON object should only contain the keys for the fields you were able to extract from the user's message. Do not include any other text or formatting.
    3.  **No Extraneous Keys**: If a value for a key is not explicitly mentioned or inferred from the user's input, DO NOT include that key in the JSON response.
    4.  **Schema and Rules**: Use the following database schema and rules to guide your extraction and validation.
        -   **data_flow_l0_detail**:
            -   SOURCE (STRING, required)
            -   SOURCE_OBJ_SCHEMA (STRING, required)
            -   SOURCE_OBJ_NAME (STRING, required)
            -   LOB (STRING, required)
            -   INPUT_FILE_FORMAT (STRING, required)
            -   STORAGE_TYPE (STRING, required): Allowed values: R, C.
            -   DQ_LOGIC (STRING, required)
            -   CDC_LOGIC (STRING, required)
            -   TRANSFORM_QUERY (STRING, required)
            -   LOAD_TYPE (STRING, required): Allowed values: FULL, DELTA.
            -   PRESTAG_FLAG (STRING, required): Allowed values: Y, N.
            -   CUSTOM_SCHEMA (STRING)
            -   DELIMETER (STRING)
            -   PARTITION (STRING)
            -   LS_FLAG (STRING)
            -   LS_DETAIL (STRING)
            -   IS_ACTIVE (STRING, required): Allowed values: Y, N. Default: Y.
    """,
    "l1_l2": """
    You are an expert data analyst and an intelligent conversational assistant for managing data pipeline metadata. Your task is to extract specific field values from a user's plain English input for the L1 or L2 layer.

    You MUST follow these rules:
    1.  **Extract Data**: Parse the user's input to extract values for the specified fields.
    2.  **Strict Output Format**: You MUST respond with a single JSON object. The JSON object should only contain the keys for the fields you were able to extract from the user's message. Do not include any other text or formatting.
    3.  **No Extraneous Keys**: If a value for a key is not explicitly mentioned or inferred from the user's input, DO NOT include that key in the JSON response.
    4.  **Schema and Rules**: Use the following database schema and rules to guide your extraction and validation.
        -   **data_flow_pb_detail**:
            -   LOB (STRING, required)
            -   TARGET_OBJ_SCHEMA (STRING, required)
            -   TARGET_OBJ_NAME (STRING, required)
            -   PRIORITY (STRING, required): Allowed values: Low, Medium, High, Critical.
            -   TARGET_OBJ_TYPE (STRING, required): Allowed values: Table, MV. Dependency: If Table, TRIGGER_TYPE must be JOB. If MV, TRIGGER_TYPE must be DLT.
            -   TRANSFORM_QUERY (STRING, required)
            -   LOAD_TYPE (STRING, required): Allowed values: FULL, DELTA, SCD. Dependency: If SCD, CUSTOM_SCRIPT_PARAMS becomes mandatory.
            -   PARTITION_METHOD (STRING, optional): Allowed values: Partition, Liquid cluster.
            -   CUSTOM_SCRIPT_PARAMS (STRING, optional)
            -   GENERIC_SCRIPTS (STRING)
            -   SOURCE_PK (STRING)
            -   TARGET_PK (STRING)
            -   PARTITION_OR_INDEX (STRING)
            -   RETENTION_DETAILS (STRING)
            -   - IS_ACTIVE (STRING, required): Allowed values: Y, N. Default: Y.
    """,
    "show_table": """
    You are an expert data analyst and an intelligent conversational assistant for managing data pipeline metadata. Your task is to identify a user's request to 'show' a pipeline's details.

    You MUST follow these rules:
    1.  **Identify Action**: Look for keywords like 'show table', 'display all pipelines', 'list pipelines'.
    2.  **Strict Output Format**: You MUST respond with a single JSON object. The JSON object should have a key `action` with the value `show_all_pipelines`.
    3.  **No Other Keys**: Do not include any other keys in the JSON response.
    4.  **No Other Text**: Do not include any other text or conversational phrases.
    """,
    "show_details": """
    You are an expert data analyst and an intelligent conversational assistant for managing data pipeline metadata. Your task is to identify a user's request to 'show' a pipeline's specific details.

    You MUST follow these rules:
    1.  **Identify Action**: Look for keywords like 'show details', 'view', 'look up', 'get details'.
    2.  **Extract Data**: Extract the **DATA_FLOW_GROUP_ID** from the user's input.
    3.  **Strict Output Format**: You MUST respond with a single JSON object. The JSON object should have a key `action` with the value `show_details` and a key `DATA_FLOW_GROUP_ID` with the extracted value.
    4.  **No Other Keys**: Do not include any other keys in the JSON response.
    5.  **No Other Text**: Do not include any other text or conversational phrases.
    """
}

REQUIRED_FIELDS_HEADER = [
    "DATA_FLOW_GROUP_ID", "BUSINESS_UNIT", "BUSINESS_OBJECT_NAME", "TRIGGER_TYPE",
    "ETL_LAYER", "COMPUTE_CLASS", "COMPUTE_CLASS_DEV", "DATA_SME", "PRODUCT_OWNER",
    "INGESTION_BUCKET", "WARNING_THRESHOLD_MINS", "WARNING_DL_GROUP", "IS_ACTIVE"
]

REQUIRED_FIELDS_L0 = [
    "SOURCE", "SOURCE_OBJ_SCHEMA", "SOURCE_OBJ_NAME", "LOB", "INPUT_FILE_FORMAT",
    "STORAGE_TYPE", "DQ_LOGIC", "CDC_LOGIC", "TRANSFORM_QUERY", "LOAD_TYPE",
    "PRESTAG_FLAG", "IS_ACTIVE"
]

REQUIRED_FIELDS_PB = [
    "LOB", "TARGET_OBJ_SCHEMA", "TARGET_OBJ_NAME", "PRIORITY", "TARGET_OBJ_TYPE",
    "TRANSFORM_QUERY", "LOAD_TYPE", "IS_ACTIVE"
]

# All possible fields for each table, including optional ones
ALL_FIELDS_HEADER = REQUIRED_FIELDS_HEADER + ["INGESTION_MODE", "SPARK_CONFIGS", "COST_CENTER", "min_version", "max_version"]
ALL_FIELDS_L0 = REQUIRED_FIELDS_L0 + ["CUSTOM_SCHEMA", "DELIMETER", "PARTITION", "LS_FLAG", "LS_DETAIL"]
ALL_FIELDS_PB = REQUIRED_FIELDS_PB + ["PARTITION_METHOD", "CUSTOM_SCRIPT_PARAMS", "GENERIC_SCRIPTS", "SOURCE_PK", "TARGET_PK", "PARTITION_OR_INDEX", "RETENTION_DETAILS"]

# Map for L0 table numbers in modify mode
L0_TABLE_MAP = {
    'table1': 0, 't1': 0,
    'table2': 1, 't2': 1,
    'table3': 2, 't3': 2,
    'table4': 3, 't4': 3,
    'table5': 4, 't5': 4
}


def get_required_fields(table_type, etl_layer=None):
    if table_type == "header":
        required = REQUIRED_FIELDS_HEADER.copy()
        if etl_layer and etl_layer.upper() == "L0":
            required.append("INGESTION_MODE")
        return required
    elif table_type == "l0":
        return REQUIRED_FIELDS_L0
    elif table_type == "pb":
        return REQUIRED_FIELDS_PB
    return []

def get_all_fields(table_type):
    if table_type == "header":
        return ALL_FIELDS_HEADER
    elif table_type == "l0":
        return ALL_FIELDS_L0
    elif table_type == "pb":
        return ALL_FIELDS_PB
    return []

# Function to add asterisk to important fields
def get_json_with_asterisks(data, table_type):
    important_fields_map = {
        "header": REQUIRED_FIELDS_HEADER,
        "l0": REQUIRED_FIELDS_L0,
        "pb": REQUIRED_FIELDS_PB
    }
    important_fields = important_fields_map.get(table_type, [])
    
    formatted_data = {}
    for key, value in data.items():
        if key in important_fields:
            formatted_data[f"*{key}*"] = value
        else:
            formatted_data[key] = value
    return formatted_data

def is_valid_email(email):
    return re.match(r"[^@]+@[^@]+\.[^@]+", email)

def validate_data(data, table_type, trigger_type=None):
    errors = []
    
    if table_type == "header":
        required_fields = get_required_fields(table_type, data.get('ETL_LAYER'))
        for field in required_fields:
            if field not in data or data[field] is None:
                errors.append(f"Missing required field: {field}")
        if "PRODUCT_OWNER" in data and not is_valid_email(data["PRODUCT_OWNER"]):
            errors.append("PRODUCT_OWNER must be a valid email address.")
        if "ETL_LAYER" in data and data["ETL_LAYER"].upper() == "L0" and "TRIGGER_TYPE" in data and data["TRIGGER_TYPE"].upper() != "DLT":
            errors.append("Dependency rule violation: TRIGGER_TYPE must be 'DLT' for ETL_LAYER 'L0'.")
            
    elif table_type == "l0":
        required_fields = get_required_fields(table_type)
        for field in required_fields:
            if field not in data or data[field] is None:
                errors.append(f"Missing required field: {field}")
    elif table_type == "pb":
        required_fields = get_required_fields(table_type)
        for field in required_fields:
            if field not in data or data[field] is None:
                errors.append(f"Missing required field: {field}")
        if "LOAD_TYPE" in data and data["LOAD_TYPE"].upper() == "SCD" and ("CUSTOM_SCRIPT_PARAMS" not in data or data["CUSTOM_SCRIPT_PARAMS"] is None):
            errors.append("Dependency rule violation: CUSTOM_SCRIPT_PARAMS is mandatory for LOAD_TYPE 'SCD'.")
        if "TARGET_OBJ_TYPE" in data and trigger_type:
            if data["TARGET_OBJ_TYPE"].upper() == "TABLE" and trigger_type.upper() != "JOB":
                errors.append("Dependency rule violation: TRIGGER_TYPE must be 'JOB' for TARGET_OBJ_TYPE 'Table'.")
            if data["TARGET_OBJ_TYPE"].upper() == "MV" and trigger_type.upper() != "DLT":
                errors.append("Dependency rule violation: TRIGGER_TYPE must be 'DLT' for TARGET_OBJ_TYPE 'MV'.")
                
    return errors

def get_pipeline_details(data_flow_group_id):
    conn = sqlite3.connect('pipelines.db')
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM data_flow_control_header WHERE DATA_FLOW_GROUP_ID = ?", (data_flow_group_id,))
    header_data = cursor.fetchone()
    
    if not header_data:
        conn.close()
        return None, None
        
    header_columns = [col[0] for col in cursor.description]
    header_dict = dict(zip(header_columns, header_data))
    
    detail_data = None
    if header_dict['ETL_LAYER'].upper() == 'L0':
        cursor.execute("SELECT * FROM data_flow_l0_detail WHERE DATA_FLOW_GROUP_ID = ?", (data_flow_group_id,))
        detail_rows = cursor.fetchall()
        if detail_rows:
            detail_columns = [col[0] for col in cursor.description]
            detail_data = [dict(zip(detail_columns, row)) for row in detail_rows]
    else:
        cursor.execute("SELECT * FROM data_flow_pb_detail WHERE DATA_FLOW_GROUP_ID = ?", (data_flow_group_id,))
        detail_row = cursor.fetchone()
        if detail_row:
            detail_columns = [col[0] for col in cursor.description]
            detail_data = dict(zip(detail_columns, detail_row))
            
    conn.close()
    return header_dict, detail_data

def get_all_pipelines_summary():
    conn = sqlite3.connect('pipelines.db')
    cursor = conn.cursor()
    cursor.execute("SELECT DATA_FLOW_GROUP_ID, BUSINESS_UNIT, ETL_LAYER, PRODUCT_OWNER FROM data_flow_control_header")
    pipelines = cursor.fetchall()
    conn.close()
    return pipelines

def show():
    # Main page layout
    #st.set_page_config(layout="wide")

    if "pipeline_data" not in st.session_state:
        st.session_state.pipeline_data = {"header": {}, "detail": {}}
    if "conversation_stage" not in st.session_state:
        st.session_state.conversation_stage = "initial"
    if "messages" not in st.session_state:
        st.session_state.messages = []


    # Sidebar for displaying collected fields and submit button
    with st.sidebar:
        st.header("Collected Fields")
        is_data_complete = False
        
        # Display header data and check for completion
        st.subheader("`data_flow_control_header`")
        header_data = st.session_state.pipeline_data.get("header", {})
        header_display_data = {key: header_data.get(key, None) for key in ALL_FIELDS_HEADER}
        st.json(get_json_with_asterisks(header_display_data, "header"))
        
        header_required_fields = REQUIRED_FIELDS_HEADER.copy()
        etl_layer = header_data.get('ETL_LAYER', '').upper()
        if etl_layer == "L0":
            header_required_fields.append("INGESTION_MODE")
        is_header_complete = all(header_data.get(field) is not None for field in header_required_fields)

        # Display detail layer data and check for completion based on ETL_LAYER
        is_detail_complete = False
        detail_data = st.session_state.pipeline_data.get("detail", {})

        if etl_layer == "L0":
            if isinstance(detail_data, list):
                st.subheader(f"`data_flow_l0_detail` (No. of Tables: {len(detail_data)})")
                is_all_l0_complete = True
                for i, table_data in enumerate(detail_data):
                    st.write(f"**Table {i+1}**:")
                    l0_display_data = {key: table_data.get(key, None) for key in ALL_FIELDS_L0}
                    st.json(get_json_with_asterisks(l0_display_data, "l0"))
                    is_current_l0_complete = all(table_data.get(field) is not None for field in REQUIRED_FIELDS_L0)
                    if not is_current_l0_complete:
                        is_all_l0_complete = False
                is_detail_complete = is_all_l0_complete
            else:
                st.subheader("`data_flow_l0_detail`")
                st.json({key: None for key in ALL_FIELDS_L0})

        elif etl_layer in ["L1", "L2"]:
            st.subheader("`data_flow_pb_detail`")
            if isinstance(detail_data, dict):
                detail_display_data = {key: detail_data.get(key, None) for key in ALL_FIELDS_PB}
                st.json(get_json_with_asterisks(detail_display_data, "pb"))
                is_detail_complete = all(detail_data.get(field) is not None for field in REQUIRED_FIELDS_PB)
            else:
                st.json({key: None for key in ALL_FIELDS_PB})
        
        # Check if both header and detail are complete
        if etl_layer in ["L0", "L1", "L2"]:
            is_data_complete = is_header_complete and is_detail_complete
        else:
            is_data_complete = is_header_complete

    # # Submit button logic
    #     if st.button("Submit", disabled=not is_data_complete, use_container_width=True):
    #         if is_data_complete:
    #             try:
    #                 # Add INSERTED_BY and UPDATED_BY fields before saving
    #                 header_data = st.session_state.pipeline_data['header']
    #                 header_data['INSERTED_BY'] = 'AI_Assistant'
    #                 header_data['UPDATED_BY'] = 'AI_Assistant'
                    
    #                 # Remove 'no_of_tables' if it exists before saving the header
    #                 header_to_save = header_data.copy()
    #                 if 'no_of_tables' in header_to_save:
    #                     del header_to_save['no_of_tables']
                    
    #                 # Save the header data first
    #                 db.save_to_db("data_flow_control_header", header_to_save)
                    
    #                 # Check the ETL layer and save the detail data accordingly
    #                 if 'ETL_LAYER' in header_data and header_data['ETL_LAYER'].upper() == "L0":
    #                     detail_data = st.session_state.pipeline_data['detail']
    #                     if isinstance(detail_data, list):
    #                         for detail_entry in detail_data:
    #                             detail_entry['INSERTED_BY'] = 'AI_Assistant'
    #                             detail_entry['UPDATED_BY'] = 'AI_Assistant'
    #                             detail_entry['DATA_FLOW_GROUP_ID'] = header_data['DATA_FLOW_GROUP_ID']
    #                             db.save_to_db("data_flow_l0_detail", detail_entry)
    #                     else:
    #                         detail_data['INSERTED_BY'] = 'AI_Assistant'
    #                         detail_data['UPDATED_BY'] = 'AI_Assistant'
    #                         detail_data['DATA_FLOW_GROUP_ID'] = header_data['DATA_FLOW_GROUP_ID']
    #                         db.save_to_db("data_flow_l0_detail", detail_data)
                    
    #                 elif 'ETL_LAYER' in header_data and header_data['ETL_LAYER'].upper() in ["L1", "L2"]:
    #                     detail_data = st.session_state.pipeline_data['detail']
    #                     detail_data['INSERTED_BY'] = 'AI_Assistant'
    #                     detail_data['UPDATED_BY'] = 'AI_Assistant'
    #                     detail_data['DATA_FLOW_GROUP_ID'] = header_data['DATA_FLOW_GROUP_ID']
    #                     db.save_to_db("data_flow_pb_detail", detail_data)
                    
    #                 st.session_state.messages.append({"role": "assistant", "content": "Data successfully saved to the database!"})
    #                 st.session_state.conversation_stage = "completed"
    #                 st.session_state.pipeline_data = {"header": {}, "detail": {}}
    #                 st.rerun()
    
    # In ai_assistant.py, within the `with st.sidebar:` block
        if st.button("Submit", disabled=not is_data_complete, use_container_width=True):
            if is_data_complete:
                st.session_state.current_view = 'add_edit'
                st.session_state.form_visible = True
                st.session_state.ai_collected_data = st.session_state.pipeline_data
                st.session_state.edit_pipeline_id = None # Ensure it's not in edit mode
                st.session_state.messages = [] # Clear the chat history
                st.session_state.pipeline_data = {"header": {}, "detail": {}} # Reset AI data
                st.session_state.conversation_stage = "initial"
                st.rerun()
            else:
                st.warning("Please complete all required fields before submitting.")

            #     except Exception as e:
            #         st.error(f"Failed to save to database: {e}")
            #         st.session_state.messages.append({"role": "assistant", "content": "Failed to save the pipeline. Please check the logs."})
            #         st.session_state.conversation_stage = "completed"
            #         st.rerun()
            # else:
            #     st.warning("Please complete all required fields before submitting.") 

    # Main content area for the conversation
    st.markdown("## Data Pipeline Assistant")
    st.markdown("---") 

    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Check to display welcome message only once
    if not st.session_state.messages:
        st.session_state.messages.append({"role": "assistant", "content": "Hello! I'm an AI assistant to help you create a new data pipeline. You can create a new pipeline or view/modify an existing one."})
        st.session_state.conversation_stage = "in_progress"
        st.rerun()

    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    if prompt := st.chat_input("Enter pipeline details..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        prompt_to_parse = prompt
        prompt_key = ""

        if prompt.lower().strip().startswith("create"):
            st.session_state.pipeline_data = {"header": {}, "detail": {}}
            st.session_state.conversation_stage = "header_in_progress"
            prompt_to_parse = prompt[len("create"):].strip()
            
            if not prompt_to_parse:
                st.session_state.messages.append({"role": "assistant", "content": "Okay, let's create a new pipeline. Please provide the details."})
                st.rerun()

        if st.session_state.conversation_stage == "completed":
            if "create" in prompt.lower():
                st.session_state.pipeline_data = {"header": {}, "detail": {}}
                st.session_state.conversation_stage = "header_in_progress"
                st.session_state.messages.append({"role": "assistant", "content": "Okay, let's create a new pipeline. Please provide the details."})
            elif "show table" in prompt.lower() or "display all pipelines" in prompt.lower() or "list pipelines" in prompt.lower():
                prompt_type = "show_all"
            elif any(keyword in prompt.lower() for keyword in ["show details", "view", "look up", "get details", "modify", "change"]):
                prompt_type = "show_details"
            else:
                st.session_state.messages.append({"role": "assistant", "content": "I'm sorry, I couldn't understand that command. Would you like to `create` a new pipeline or `show table`?"})
            st.rerun()
        
        if st.session_state.conversation_stage != "completed":
            
            if st.session_state.conversation_stage in ["header_in_progress", "in_progress"]:
                prompt_key = 'header'
            elif st.session_state.conversation_stage in ["detail_l0_in_progress", "l0_num_tables_in_progress"]:
                prompt_key = 'l0'
            elif st.session_state.conversation_stage in ["detail_pb_in_progress"]:
                prompt_key = 'l1_l2'
            elif st.session_state.conversation_stage in ["modify", "modifying"]:
                header_data = st.session_state.pipeline_data.get('header', {})
                etl_layer = header_data.get('ETL_LAYER', '').lower()
                if etl_layer == 'l0':
                    prompt_key = 'l0'
                elif etl_layer in ['l1', 'l2']:
                    prompt_key = 'l1_l2'
                else:
                    prompt_key = 'header'
            else:
                prompt_key = 'header'

            prompt_type = "create"
            if "show table" in prompt_to_parse.lower() or "display all pipelines" in prompt_to_parse.lower() or "list pipelines" in prompt_to_parse.lower():
                prompt_type = "show_all"
            elif any(keyword in prompt_to_parse.lower() for keyword in ["show details", "view", "look up", "get details", "modify", "change"]):
                prompt_type = "show_details"
            
            if prompt_type == "show_all":
                with st.chat_message("assistant"):
                    pipelines = get_all_pipelines_summary()
                    if pipelines:
                        summary_markdown = "Here is a list of all existing pipelines:\n\n"
                        for p in pipelines:
                            summary_markdown += f"- **{p[0]}**: BU: `{p[1]}`, Layer: `{p[2]}`, Owner: `{p[3]}`\n"
                        summary_markdown += "\nTo see more details, please specify a `DATA_FLOW_GROUP_ID` (e.g., 'show details for retail_sales_dwh')."
                        st.markdown(summary_markdown)
                        st.session_state.messages.append({"role": "assistant", "content": summary_markdown})
                    else:
                        st.markdown("There are no pipelines in the database yet.")
                        st.session_state.messages.append({"role": "assistant", "content": "There are no pipelines in the database yet."})
                st.rerun()
            
            elif prompt_type == "show_details":
                with st.chat_message("assistant"):
                    full_prompt = f"{SYSTEM_PROMPTS['show_details' or 'show_detail']}\nUser input: {prompt_to_parse}"
                    response = model.generate_content(full_prompt)
                    
                    try:
                        response_text = response.text.replace("```json", "").replace("```", "").strip()
                        extracted_data = json.loads(response_text)
                        
                        if extracted_data.get("action") == "show_details" and "DATA_FLOW_GROUP_ID" in extracted_data:
                            data_flow_group_id = extracted_data["DATA_FLOW_GROUP_ID"]
                            header_data, detail_data = get_pipeline_details(data_flow_group_id)
                            
                            if header_data:
                                st.session_state.pipeline_data['header'] = header_data
                                st.session_state.pipeline_data['detail'] = detail_data
                                
                                st.session_state.conversation_stage = "modify"
                                st.markdown("Details are in the side panel. Would you like to modify table fields? (Yes/No)")
                                st.session_state.messages.append({"role": "assistant", "content": "Details are in the side panel. Would you like to modify table fields? (Yes/No)"})

                            else:
                                st.markdown(f"I could not find a pipeline with the ID `{data_flow_group_id}`. Please check the ID and try again.")
                                st.session_state.messages.append({"role": "assistant", "content": f"I could not find a pipeline with the ID `{data_flow_group_id}`. Please check the ID and try again."})

                        else:
                            st.markdown("I'm sorry, I couldn't understand that request. Please try to phrase it clearly, for example: 'show me the details for pipeline [ID]'.")
                            st.session_state.messages.append({"role": "assistant", "content": "I'm sorry, I couldn't understand that request. Please try to phrase it clearly, for example: 'show me the details for pipeline [ID]'."})
                            
                    except (json.JSONDecodeError, ValueError) as e:
                        st.markdown(f"An unexpected error occurred: {e}. Please try again.")
                        st.session_state.messages.append({"role": "assistant", "content": f"An unexpected error occurred: {e}. Please try again."})
                    st.rerun() 

            elif st.session_state.conversation_stage == "modify":
                if prompt.strip().lower() == "yes":
                    st.session_state.conversation_stage = "modifying"
                    st.markdown("Which field would you like to change?")
                    st.session_state.messages.append({"role": "assistant", "content": "Which field would you like to change?"})
                else:
                    st.session_state.conversation_stage = "completed"
                    st.markdown("Okay, no changes will be made.")
                    st.session_state.messages.append({"role": "assistant", "content": "Okay, no changes will be made."})
                st.rerun()

            elif st.session_state.conversation_stage == "modifying":
                with st.chat_message("assistant"):
                    header_data = st.session_state.pipeline_data.get('header', {})
                    etl_layer = header_data.get('ETL_LAYER', '').lower()
                    
                    if etl_layer == 'l0':
                        prompt_key = 'l0'
                    elif etl_layer in ['l1', 'l2']:
                        prompt_key = 'l1_l2'
                    else:
                        prompt_key = 'header'

                    full_prompt = f"{SYSTEM_PROMPTS[prompt_key]}\nUser input: {prompt}"
                    response = model.generate_content(full_prompt)
                    
                    try:
                        response_text = response.text.replace("```json", "").replace("```", "").strip()
                        if not response_text:
                            raise ValueError("Empty response from the model.")
                        
                        extracted_data = json.loads(response_text)
                        
                        updated = False
                        
                        if etl_layer == 'l0':
                            table_index = -1
                            for map_key, index in L0_TABLE_MAP.items():
                                if map_key in prompt.lower():
                                    table_index = index
                                    break
                            
                            if table_index != -1 and isinstance(st.session_state.pipeline_data.get('detail'), list):
                                if len(st.session_state.pipeline_data['detail']) > table_index:
                                    for key, value in extracted_data.items():
                                        normalized_key = key.upper().replace(' ', '_').replace('LAYER_', '').replace('TABLE_', '')
                                        if normalized_key in ALL_FIELDS_L0:
                                            st.session_state.pipeline_data['detail'][table_index][normalized_key] = value
                                            updated = True
                                else:
                                    st.markdown(f"The specified table number (table {table_index + 1}) is out of range. Please choose a table from 1 to {len(st.session_state.pipeline_data['detail'])}.")
                                    st.session_state.messages.append({"role": "assistant", "content": f"The specified table number (table {table_index + 1}) is out of range. Please choose a table from 1 to {len(st.session_state.pipeline_data['detail'])}."})
                                    st.rerun()

                        elif etl_layer in ['l1', 'l2']:
                            if isinstance(st.session_state.pipeline_data.get('detail'), dict):
                                for key, value in extracted_data.items():
                                    normalized_key = key.upper().replace(' ', '_').replace('LAYER_', '').replace('TABLE_', '')
                                    if normalized_key in ALL_FIELDS_PB:
                                        st.session_state.pipeline_data['detail'][normalized_key] = value
                                        updated = True
                        
                        for key, value in extracted_data.items():
                            normalized_key = key.upper().replace(' ', '_').replace('HEADER_', '')
                            
                            if normalized_key == 'DATA_FLOW_GROUP_ID':
                                continue

                            if normalized_key in ALL_FIELDS_HEADER:
                                if normalized_key == 'IS_ACTIVE' and ('layer' in prompt.lower() or 'table' in prompt.lower()):
                                    continue
                                
                                st.session_state.pipeline_data['header'][normalized_key] = value
                                updated = True

                        if updated:
                            st.markdown("I have updated the pipeline details. Please review the sidebar to see the changes.")
                            st.session_state.messages.append({"role": "assistant", "content": "I have updated the pipeline details. Please review the sidebar to see the changes."})
                        else:
                            st.markdown("I couldn't find a matching field to update. Please try again.")
                            st.session_state.messages.append({"role": "assistant", "content": "I couldn't find a matching field to update. Please try again."})
                    
                    except (json.JSONDecodeError, ValueError) as e:
                        error_message = f"An unexpected error occurred: {e}. Please try again."
                        st.markdown(error_message)
                        st.session_state.messages.append({"role": "assistant", "content": error_message})
                    st.rerun()

            else:
                current_data = {}
                table_type = ""
                prompt_key = ""

                if st.session_state.conversation_stage == "in_progress":
                    st.session_state.conversation_stage = "header_in_progress"
                
                if st.session_state.conversation_stage == "header_in_progress":
                    current_data = st.session_state.pipeline_data['header']
                    table_type = "header"
                    prompt_key = "header"
                elif st.session_state.conversation_stage == "detail_l0_in_progress":
                    current_data = st.session_state.pipeline_data['detail']
                    table_type = "l0"
                    prompt_key = "l0"
                elif st.session_state.conversation_stage == "l0_num_tables_in_progress":
                    try:
                        num_tables = int(prompt.strip())
                        if 1 <= num_tables <= 5:
                            st.session_state.pipeline_data['header']['no_of_tables'] = num_tables
                            st.session_state.pipeline_data['detail'] = [{} for _ in range(num_tables)]
                            st.session_state.conversation_stage = "detail_l0_in_progress"
                            st.markdown(f"Okay, now provide details for the {num_tables} L0 tables. You can specify the table number, e.g., 'table1 source is my_source'.")
                            st.session_state.messages.append({"role": "assistant", "content": f"Okay, now provide details for the {num_tables} L0 tables. You can specify the table number, e.g., 'table1 source is my_source'."})
                        else:
                            st.markdown("Please enter a number between 1 and 5.")
                            st.session_state.messages.append({"role": "assistant", "content": "Please enter a number between 1 and 5."})
                        st.rerun()
                    except ValueError:
                        st.markdown("Invalid input. Please enter a number.")
                        st.session_state.messages.append({"role": "assistant", "content": "Invalid input. Please enter a number."})
                        st.rerun()
                elif st.session_state.conversation_stage == "detail_pb_in_progress":
                    current_data = st.session_state.pipeline_data['detail']
                    table_type = "pb"
                    prompt_key = "l1_l2"

                with st.chat_message("assistant"):
                    if st.session_state.conversation_stage in ["header_in_progress", "detail_l0_in_progress", "detail_pb_in_progress"]:
                        prompt_to_parse = prompt
                        if prompt.lower().strip().startswith("create"):
                            prompt_to_parse = prompt[len("create"):].strip()

                        full_prompt = f"{SYSTEM_PROMPTS[prompt_key]}\nUser input: {prompt_to_parse}"
                        response = model.generate_content(full_prompt)

                        try:
                            response_text = response.text.replace("```json", "").replace("```", "").strip()
                            if not response_text.strip() or not response_text.startswith('{') or not response_text.endswith('}'):
                                raise ValueError("Invalid response format from the model. Expected a JSON object.")
                            
                            extracted_data = json.loads(response_text)
                            
                            if table_type == "header":
                                st.session_state.pipeline_data['header'].update(extracted_data)
                            elif table_type == "pb":
                                st.session_state.pipeline_data['detail'].update(extracted_data)
                            elif table_type == "l0":
                                table_index = 0
                                for map_key, index in L0_TABLE_MAP.items():
                                    if map_key in prompt.lower():
                                        table_index = index
                                        break
                                
                                if isinstance(st.session_state.pipeline_data.get('detail'), list) and len(st.session_state.pipeline_data['detail']) > table_index:
                                    st.session_state.pipeline_data['detail'][table_index].update(extracted_data)
                                else:
                                    st.markdown("Please specify which L0 table you are providing details for (e.g., 'table1').")
                                    st.session_state.messages.append({"role": "assistant", "content": "Please specify which L0 table you are providing details for (e.g., 'table1')."})
                                    st.rerun()

                            validation_errors = []
                            if table_type == "header":
                                header_required_fields = get_required_fields(table_type, st.session_state.pipeline_data['header'].get('ETL_LAYER'))
                                if not all(st.session_state.pipeline_data['header'].get(field) is not None for field in header_required_fields):
                                    validation_errors.append("Missing required header fields.")
                            elif table_type == "pb":
                                pb_required_fields = get_required_fields(table_type)
                                if not all(st.session_state.pipeline_data['detail'].get(field) is not None for field in pb_required_fields):
                                    validation_errors.append("Missing required detail fields.")
                            elif table_type == "l0":
                                l0_required_fields = get_required_fields(table_type)
                                if isinstance(st.session_state.pipeline_data['detail'], list):
                                    for i, table_data in enumerate(st.session_state.pipeline_data['detail']):
                                        if not all(table_data.get(field) is not None for field in l0_required_fields):
                                            validation_errors.append(f"Missing required fields for L0 table {i+1}.")

                            if validation_errors:
                                st.markdown("Validation Errors:\n" + "\n".join(validation_errors))
                                st.session_state.messages.append({"role": "assistant", "content": "Validation Errors:\n" + "\n".join(validation_errors)})
                            else:
                                if st.session_state.conversation_stage == "header_in_progress":
                                    etl_layer_val = st.session_state.pipeline_data['header'].get('ETL_LAYER', '').upper()
                                    if etl_layer_val == 'L0':
                                        st.session_state.conversation_stage = "l0_num_tables_in_progress"
                                        st.markdown("Okay, the header is complete. How many L0 tables does this pipeline have? (Max 5)")
                                        st.session_state.messages.append({"role": "assistant", "content": "Okay, the header is complete. How many L0 tables does this pipeline have? (Max 5)"})
                                    elif etl_layer_val in ['L1', 'L2']:
                                        st.session_state.conversation_stage = "detail_pb_in_progress"
                                        st.markdown("Okay, now please provide the details for the `data_flow_pb_detail` table.")
                                        st.session_state.messages.append({"role": "assistant", "content": "Okay, now please provide the details for the `data_flow_pb_detail` table."})
                                
                                elif st.session_state.conversation_stage in ["detail_l0_in_progress", "detail_pb_in_progress"]:
                                    st.markdown("Pipeline data has been updated. All required fields are completed. You can now submit the data.")
                                    st.session_state.messages.append({"role": "assistant", "content": "All required fields are completed. You can now submit the data."})
                                
                                st.rerun()

                        except (json.JSONDecodeError, ValueError) as e:
                            error_message = f"I'm sorry, I couldn't understand that. Please try to provide the information in a clear format. The error was: {e}"
                            st.markdown(error_message)
                            st.session_state.messages.append({"role": "assistant", "content": error_message})
                            st.rerun()
                        except Exception as e:
                            st.error(f"An unexpected error occurred: {e}")
                            st.session_state.messages.append({"role": "assistant", "content": "An unexpected error occurred. Please try again."})

                            st.rerun()
