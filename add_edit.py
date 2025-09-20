import streamlit as st
import uuid
import database
import datetime

def get_select_box_index(options, value):
    """Safely gets the index of a value in a list of options."""
    if value and value in options:
        return options.index(value)
    return 0

def update_etl_layer():
    """Callback function to synchronize the ETL Layer selectbox with the pipeline type."""
    selected_layer_text = st.session_state.layer_selector
    layer_code = selected_layer_text.split(" ")[0]
    st.session_state['general_etl_layer'] = layer_code


def show(prefill_data=None):
    """
    Main function to display the application UI for creating and editing pipelines.
    Handles data pre-filling from the AI assistant or database.
    """
    # --- 1. Session State Initialization ---
    if 'form_visible' not in st.session_state:
        st.session_state['form_visible'] = False
    if 'current_pipeline_layer' not in st.session_state:
        st.session_state['current_pipeline_layer'] = "L0"
    if 'edit_pipeline_id' not in st.session_state:
        st.session_state['edit_pipeline_id'] = None
    if 'l0_num_tables' not in st.session_state:
        st.session_state['l0_num_tables'] = 1

    # --- 2. Load Data from AI or Database & Pre-fill Session State ---
    if prefill_data:
        general_data_values = prefill_data.get('header', {})
        l0_details_list = prefill_data.get('detail', [])
        pb_details_values = prefill_data.get('detail', {})
        
        st.session_state.current_pipeline_layer = str(general_data_values.get('ETL_LAYER', 'L0'))
        st.session_state.form_visible = True
        
        # --- Pre-fill session state from AI data ---
        st.session_state['general_data_flow_id'] = str(general_data_values.get('DATA_FLOW_GROUP_ID', ''))
        st.session_state['general_business_unit'] = str(general_data_values.get('BUSINESS_UNIT', ''))
        st.session_state['general_product_owner'] = str(general_data_values.get('PRODUCT_OWNER', ''))
        
        status_options = ["Y", "N"]
        st.session_state['general_is_active'] = str(general_data_values.get('IS_ACTIVE', 'Y')) if str(general_data_values.get('IS_ACTIVE', 'Y')) in status_options else status_options[0]
        
        trigger_options = ["DLT", "JOB"]
        st.session_state['general_trigger_type'] = str(general_data_values.get('TRIGGER_TYPE', 'DLT')) if str(general_data_values.get('TRIGGER_TYPE', 'DLT')) in trigger_options else trigger_options[0]
        
        etl_layer_options = ["L0", "L1", "L2"]
        st.session_state['general_etl_layer'] = str(general_data_values.get('ETL_LAYER', 'L0')) if str(general_data_values.get('ETL_LAYER', 'L0')) in etl_layer_options else etl_layer_options[0]
        
        st.session_state['general_data_sme'] = str(general_data_values.get('DATA_SME', ''))
        st.session_state['general_ingestion_mode'] = str(general_data_values.get('INGESTION_MODE', ''))
        st.session_state['general_ingestion_bucket'] = str(general_data_values.get('INGESTION_BUCKET', ''))
        st.session_state['general_warning_threshold'] = int(general_data_values.get('WARNING_THRESHOLD_MINS') or 0)
        st.session_state['general_business_object_name'] = str(general_data_values.get('BUSINESS_OBJECT_NAME', ''))
        
        compute_class_options = database.get_compute_classes(dev_allowed=False)
        st.session_state['general_compute_class'] = str(general_data_values.get('COMPUTE_CLASS', '')) if str(general_data_values.get('COMPUTE_CLASS', '')) in compute_class_options else (compute_class_options[0] if compute_class_options else '')
        
        dev_compute_class_options = database.get_compute_classes(dev_allowed=True)
        st.session_state['general_compute_class_dev'] = str(general_data_values.get('COMPUTE_CLASS_DEV', '')) if str(general_data_values.get('COMPUTE_CLASS_DEV', '')) in dev_compute_class_options else (dev_compute_class_options[0] if dev_compute_class_options else '')
        
        st.session_state['general_cost_center'] = str(general_data_values.get('COST_CENTER', ''))
        st.session_state['general_warning_dl_group'] = str(general_data_values.get('WARNING_DL_GROUP', ''))
        st.session_state['general_spark_configs'] = str(general_data_values.get('SPARK_CONFIGS', ''))
        st.session_state['general_min_version'] = float(general_data_values.get('min_version') or 0.0)
        st.session_state['general_max_version'] = float(general_data_values.get('max_version') or 0.0)
        st.session_state['general_inserted_by'] = str(general_data_values.get('INSERTED_BY', ''))
        st.session_state['general_updated_by'] = str(general_data_values.get('UPDATED_BY', ''))
        
        if st.session_state.current_pipeline_layer == 'L0':
            st.session_state.l0_num_tables = len(l0_details_list) if l0_details_list else 1
            for i in range(st.session_state.l0_num_tables):
                table_data_values = l0_details_list[i] if i < len(l0_details_list) else {}
                st.session_state[f"l0_id_{i}"] = table_data_values.get('l0_id')
                st.session_state[f"source_{i}"] = str(table_data_values.get('SOURCE', ''))
                st.session_state[f"source_obj_name_{i}"] = str(table_data_values.get('SOURCE_OBJ_NAME', ''))
                st.session_state[f"storage_type_{i}"] = str(table_data_values.get('STORAGE_TYPE', 'R')) if str(table_data_values.get('STORAGE_TYPE', 'R')) in ["R", "C"] else "R"
                st.session_state[f"source_schema_{i}"] = str(table_data_values.get('SOURCE_OBJ_SCHEMA', ''))
                st.session_state[f"input_file_format_{i}"] = str(table_data_values.get('INPUT_FILE_FORMAT', ''))
                st.session_state[f"delimeter_{i}"] = str(table_data_values.get('DELIMETER', ''))
                st.session_state[f"custom_schema_{i}"] = str(table_data_values.get('CUSTOM_SCHEMA', ''))
                st.session_state[f"dq_logic_{i}"] = str(table_data_values.get('DQ_LOGIC', ''))
                st.session_state[f"cdc_logic_{i}"] = str(table_data_values.get('CDC_LOGIC', ''))
                st.session_state[f"transform_query_{i}"] = str(table_data_values.get('TRANSFORM_QUERY', ''))
                st.session_state[f"load_type_{i}"] = str(table_data_values.get('LOAD_TYPE', 'FULL')) if str(table_data_values.get('LOAD_TYPE', 'FULL')) in ["FULL", "DELTA"] else "FULL"
                st.session_state[f"prestag_flag_{i}"] = str(table_data_values.get('PRESTAG_FLAG', 'Y')) if str(table_data_values.get('PRESTAG_FLAG', 'Y')) in ["Y", "N"] else "Y"
                st.session_state[f"lob_{i}"] = str(table_data_values.get('LOB', ''))
                st.session_state[f"partition_{i}"] = str(table_data_values.get('PARTITION', ''))
                st.session_state[f"status_{i}"] = str(table_data_values.get('IS_ACTIVE', 'Y')) if str(table_data_values.get('IS_ACTIVE', 'Y')) in ["Y", "N"] else "Y"
                st.session_state[f"ls_flag_{i}"] = str(table_data_values.get('LS_FLAG', 'N')) if str(table_data_values.get('LS_FLAG', 'N')) in ["Y", "N"] else "N"
                st.session_state[f"ls_detail_{i}"] = str(table_data_values.get('LS_DETAIL', ''))
        else: # L1 or L2
            pb_data_values = pb_details_values[0] if isinstance(pb_details_values, list) and pb_details_values else (pb_details_values if isinstance(pb_details_values, dict) else {})
            st.session_state['pb_id'] = pb_data_values.get('pb_id')
            
            target_obj_options = ["Table", "MV", "Spark"]
            st.session_state['target_obj_type_L1_L2'] = str(pb_data_values.get('TARGET_OBJ_TYPE', 'Table'))
            if st.session_state.get('target_obj_type_L1_L2') not in target_obj_options:
                 st.session_state['target_obj_type_L1_L2'] = target_obj_options[0]
            
            st.session_state['lob_L1_L2'] = str(pb_data_values.get('LOB', ''))
            st.session_state['source_pk_L1_L2'] = str(pb_data_values.get('SOURCE_PK', ''))
            st.session_state['target_obj_schema_L1_L2'] = str(pb_data_values.get('TARGET_OBJ_SCHEMA', ''))
            
            priority_options = ["Low", "Medium", "High", "Critical"]
            st.session_state['priority_L1_L2'] = str(pb_data_values.get('PRIORITY', 'Medium'))
            if st.session_state.get('priority_L1_L2') not in priority_options:
                 st.session_state['priority_L1_L2'] = priority_options[1]
            
            st.session_state['transform_query_L1_L2'] = str(pb_data_values.get('TRANSFORM_QUERY', ''))
            st.session_state['target_obj_name_L1_L2'] = str(pb_data_values.get('TARGET_OBJ_NAME', ''))
            st.session_state['generic_scripts_L1_L2'] = str(pb_data_values.get('GENERIC_SCRIPTS', ''))
            st.session_state['target_pk_L1_L2'] = str(pb_data_values.get('TARGET_PK', ''))
            
            load_type_options = ["FULL", "DELTA", "SCD"]
            st.session_state['load_type_L1_L2'] = str(pb_data_values.get('LOAD_TYPE', 'FULL'))
            if st.session_state.get('load_type_L1_L2') not in load_type_options:
                 st.session_state['load_type_L1_L2'] = load_type_options[0]
            
            partition_method_options = ["Partition", "Liquid cluster"]
            st.session_state['partition_method_L1_L2'] = str(pb_data_values.get('PARTITION_METHOD', 'Partition'))
            if st.session_state.get('partition_method_L1_L2') not in partition_method_options:
                 st.session_state['partition_method_L1_L2'] = partition_method_options[0]
            
            st.session_state['partition_or_index_L1_L2'] = str(pb_data_values.get('PARTITION_OR_INDEX', ''))
            st.session_state['custom_script_params_L1_L2'] = str(pb_data_values.get('CUSTOM_SCRIPT_PARAMS', ''))
            st.session_state['retention_details_L1_L2'] = str(pb_data_values.get('RETENTION_DETAILS', ''))
            
            status_options = ["Y", "N"]
            st.session_state['status_L1_L2'] = str(pb_data_values.get('IS_ACTIVE', 'Y'))
            if st.session_state.get('status_L1_L2') not in status_options:
                 st.session_state['status_L1_L2'] = status_options[0]

        st.info("AI data loaded. You can edit it now.")
        
    elif st.session_state.edit_pipeline_id:
        current_pipeline = database.get_pipeline_by_id(st.session_state.edit_pipeline_id)
        if not current_pipeline:
            st.error("Pipeline not found.")
            st.session_state.form_visible = False
            st.session_state.edit_pipeline_id = None
            st.rerun()
        
        general_data_values = current_pipeline
        l0_details_list = current_pipeline.get('l0_details', [])
        pb_details_values = current_pipeline.get('pb_details', [])
        st.session_state.current_pipeline_layer = str(general_data_values.get('ETL_LAYER', 'L0'))
        
        # --- Pre-fill session state from DB data ---
        st.session_state['general_data_flow_id'] = str(general_data_values.get('DATA_FLOW_GROUP_ID', ''))
        st.session_state['general_business_unit'] = str(general_data_values.get('BUSINESS_UNIT', ''))
        st.session_state['general_product_owner'] = str(general_data_values.get('PRODUCT_OWNER', ''))
        
        status_options = ["Y", "N"]
        st.session_state['general_is_active'] = str(general_data_values.get('IS_ACTIVE', 'Y')) if str(general_data_values.get('IS_ACTIVE', 'Y')) in status_options else status_options[0]
        
        trigger_options = ["DLT", "JOB"]
        st.session_state['general_trigger_type'] = str(general_data_values.get('TRIGGER_TYPE', 'DLT')) if str(general_data_values.get('TRIGGER_TYPE', 'DLT')) in trigger_options else trigger_options[0]
        
        etl_layer_options = ["L0", "L1", "L2"]
        st.session_state['general_etl_layer'] = str(general_data_values.get('ETL_LAYER', 'L0')) if str(general_data_values.get('ETL_LAYER', 'L0')) in etl_layer_options else etl_layer_options[0]
        
        st.session_state['general_data_sme'] = str(general_data_values.get('DATA_SME', ''))
        st.session_state['general_ingestion_mode'] = str(general_data_values.get('INGESTION_MODE', ''))
        st.session_state['general_ingestion_bucket'] = str(general_data_values.get('INGESTION_BUCKET', ''))
        st.session_state['general_warning_threshold'] = int(general_data_values.get('WARNING_THRESHOLD_MINS') or 0)
        st.session_state['general_business_object_name'] = str(general_data_values.get('BUSINESS_OBJECT_NAME', ''))
        
        compute_class_options = database.get_compute_classes(dev_allowed=False)
        st.session_state['general_compute_class'] = str(general_data_values.get('COMPUTE_CLASS', '')) if str(general_data_values.get('COMPUTE_CLASS', '')) in compute_class_options else (compute_class_options[0] if compute_class_options else '')
        
        dev_compute_class_options = database.get_compute_classes(dev_allowed=True)
        st.session_state['general_compute_class_dev'] = str(general_data_values.get('COMPUTE_CLASS_DEV', '')) if str(general_data_values.get('COMPUTE_CLASS_DEV', '')) in dev_compute_class_options else (dev_compute_class_options[0] if dev_compute_class_options else '')
        
        st.session_state['general_cost_center'] = str(general_data_values.get('COST_CENTER', ''))
        st.session_state['general_warning_dl_group'] = str(general_data_values.get('WARNING_DL_GROUP', ''))
        st.session_state['general_spark_configs'] = str(general_data_values.get('SPARK_CONFIGS', ''))
        st.session_state['general_min_version'] = float(general_data_values.get('min_version') or 0.0)
        st.session_state['general_max_version'] = float(general_data_values.get('max_version') or 0.0)
        st.session_state['general_inserted_by'] = str(general_data_values.get('INSERTED_BY', ''))
        st.session_state['general_updated_by'] = str(general_data_values.get('UPDATED_BY', ''))
        
        if st.session_state.current_pipeline_layer == "L0":
            st.session_state.l0_num_tables = len(l0_details_list) if l0_details_list else 1
            for i in range(st.session_state.l0_num_tables):
                table_data_values = l0_details_list[i] if i < len(l0_details_list) else {}
                st.session_state[f"l0_id_{i}"] = table_data_values.get('l0_id')
                st.session_state[f"source_{i}"] = str(table_data_values.get('SOURCE', ''))
                st.session_state[f"source_obj_name_{i}"] = str(table_data_values.get('SOURCE_OBJ_NAME', ''))
                st.session_state[f"storage_type_{i}"] = str(table_data_values.get('STORAGE_TYPE', 'R')) if str(table_data_values.get('STORAGE_TYPE', 'R')) in ["R", "C"] else "R"
                st.session_state[f"source_schema_{i}"] = str(table_data_values.get('SOURCE_OBJ_SCHEMA', ''))
                st.session_state[f"input_file_format_{i}"] = str(table_data_values.get('INPUT_FILE_FORMAT', ''))
                st.session_state[f"delimeter_{i}"] = str(table_data_values.get('DELIMETER', ''))
                st.session_state[f"custom_schema_{i}"] = str(table_data_values.get('CUSTOM_SCHEMA', ''))
                st.session_state[f"dq_logic_{i}"] = str(table_data_values.get('DQ_LOGIC', ''))
                st.session_state[f"cdc_logic_{i}"] = str(table_data_values.get('CDC_LOGIC', ''))
                st.session_state[f"transform_query_{i}"] = str(table_data_values.get('TRANSFORM_QUERY', ''))
                st.session_state[f"load_type_{i}"] = str(table_data_values.get('LOAD_TYPE', 'FULL')) if str(table_data_values.get('LOAD_TYPE', 'FULL')) in ["FULL", "DELTA"] else "FULL"
                st.session_state[f"prestag_flag_{i}"] = str(table_data_values.get('PRESTAG_FLAG', 'Y')) if str(table_data_values.get('PRESTAG_FLAG', 'Y')) in ["Y", "N"] else "Y"
                st.session_state[f"lob_{i}"] = str(table_data_values.get('LOB', ''))
                st.session_state[f"partition_{i}"] = str(table_data_values.get('PARTITION', ''))
                st.session_state[f"status_{i}"] = str(table_data_values.get('IS_ACTIVE', 'Y')) if str(table_data_values.get('IS_ACTIVE', 'Y')) in ["Y", "N"] else "Y"
                st.session_state[f"ls_flag_{i}"] = str(table_data_values.get('LS_FLAG', 'N')) if str(table_data_values.get('LS_FLAG', 'N')) in ["Y", "N"] else "N"
                st.session_state[f"ls_detail_{i}"] = str(table_data_values.get('LS_DETAIL', ''))
        else: # L1 or L2
            pb_details_list = pb_details_values if isinstance(pb_details_values, list) else [pb_details_values]
            pb_data_values = pb_details_list[0] if pb_details_list else {}
            st.session_state['pb_id'] = pb_data_values.get('pb_id')
            
            target_obj_options = ["Table", "MV", "Spark"]
            st.session_state['target_obj_type_L1_L2'] = str(pb_data_values.get('TARGET_OBJ_TYPE', 'Table'))
            if st.session_state.get('target_obj_type_L1_L2') not in target_obj_options:
                 st.session_state['target_obj_type_L1_L2'] = target_obj_options[0]
            
            st.session_state['lob_L1_L2'] = str(pb_data_values.get('LOB', ''))
            st.session_state['source_pk_L1_L2'] = str(pb_data_values.get('SOURCE_PK', ''))
            st.session_state['target_obj_schema_L1_L2'] = str(pb_data_values.get('TARGET_OBJ_SCHEMA', ''))
            
            priority_options = ["Low", "Medium", "High", "Critical"]
            st.session_state['priority_L1_L2'] = str(pb_data_values.get('PRIORITY', 'Medium'))
            if st.session_state.get('priority_L1_L2') not in priority_options:
                 st.session_state['priority_L1_L2'] = priority_options[1]
            
            st.session_state['transform_query_L1_L2'] = str(pb_data_values.get('TRANSFORM_QUERY', ''))
            st.session_state['target_obj_name_L1_L2'] = str(pb_data_values.get('TARGET_OBJ_NAME', ''))
            st.session_state['generic_scripts_L1_L2'] = str(pb_data_values.get('GENERIC_SCRIPTS', ''))
            st.session_state['target_pk_L1_L2'] = str(pb_data_values.get('TARGET_PK', ''))
            
            load_type_options = ["FULL", "DELTA", "SCD"]
            st.session_state['load_type_L1_L2'] = str(pb_data_values.get('LOAD_TYPE', 'FULL'))
            if st.session_state.get('load_type_L1_L2') not in load_type_options:
                 st.session_state['load_type_L1_L2'] = load_type_options[0]
            
            partition_method_options = ["Partition", "Liquid cluster"]
            st.session_state['partition_method_L1_L2'] = str(pb_data_values.get('PARTITION_METHOD', 'Partition'))
            if st.session_state.get('partition_method_L1_L2') not in partition_method_options:
                 st.session_state['partition_method_L1_L2'] = partition_method_options[0]
            
            st.session_state['partition_or_index_L1_L2'] = str(pb_data_values.get('PARTITION_OR_INDEX', ''))
            st.session_state['custom_script_params_L1_L2'] = str(pb_data_values.get('CUSTOM_SCRIPT_PARAMS', ''))
            st.session_state['retention_details_L1_L2'] = str(pb_data_values.get('RETENTION_DETAILS', ''))
            
            status_options = ["Y", "N"]
            st.session_state['status_L1_L2'] = str(pb_data_values.get('IS_ACTIVE', 'Y'))
            if st.session_state.get('status_L1_L2') not in status_options:
                 st.session_state['status_L1_L2'] = status_options[0]

        st.info("Pipeline data loaded from database. You can edit it now.")
        

    if st.session_state.form_visible:
        if st.button("← Back to Dashboard"):
            st.session_state.form_visible = False
            st.session_state.edit_pipeline_id = None
            st.rerun()
            
        st.subheader("Create/Edit Pipeline Metadata")
        layer_options = ["L0 Raw Layer", "L1 Curated Layer", "L2 Data Product Layer"]
        selected_layer_index = get_select_box_index([item.split(' ')[0] for item in layer_options], st.session_state.get('current_pipeline_layer', 'L0'))
        st.selectbox(
            "Select Pipeline Type/Layer",
            layer_options,
            index=selected_layer_index,
            key='layer_selector',
            on_change=update_etl_layer
        )
        st.session_state.current_pipeline_layer = st.session_state.layer_selector.split(" ")[0]
        current_layer = st.session_state.current_pipeline_layer

        with st.form("pipeline_main_form"):
            # --- General Configuration Section ---
            with st.container(border=True):
                st.subheader("General Information")
                st.text_input("Dataflow Name *", value=st.session_state.get('general_data_flow_id', ''), disabled=bool(st.session_state.edit_pipeline_id), key="general_data_flow_id")
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.text_input("Business Unit *", value=st.session_state.get('general_business_unit', ''), key="general_business_unit")
                with col2:
                    st.text_input("Product Owner * (e.g., your_name@company.com)", value=st.session_state.get('general_product_owner', ''), placeholder="e.g., jane.doe@company.com", key="general_product_owner")
                with col3:
                    status_options = ["Y", "N"]
                    st.selectbox("Statues *", status_options, index=get_select_box_index(status_options, st.session_state.get("general_is_active", "Y")), key="general_is_active")
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    trigger_options = ["DLT", "JOB"]
                    st.selectbox("Trigger type *", trigger_options, index=get_select_box_index(trigger_options, st.session_state.get('general_trigger_type', "DLT")), key="general_trigger_type")
                    
                    etl_layer_options = ["L0", "L1", "L2"]
                    st.selectbox("ETL Layer *", etl_layer_options, index=get_select_box_index(etl_layer_options, st.session_state.get('general_etl_layer', "L0")), key="general_etl_layer", disabled=True)
                    
                    st.text_input("Data SME *", value=st.session_state.get('general_data_sme', ''), key="general_data_sme")
                    st.text_input("Ingestion Mode *", value=st.session_state.get('general_ingestion_mode', ''), key="general_ingestion_mode")
                    st.text_input("Ingestion bucket", value=st.session_state.get('general_ingestion_bucket', ''), key="general_ingestion_bucket")
                    
                    st.number_input("Warning threshold(minutes) *", min_value=0, value=st.session_state.get('general_warning_threshold', 0), key="general_warning_threshold")

                with col2:
                    st.text_input("Business Object Name", value=st.session_state.get('general_business_object_name', ''), key="general_business_object_name")
                    
                    compute_class_options = database.get_compute_classes(dev_allowed=False)
                    st.selectbox("Compute class *", compute_class_options, index=get_select_box_index(compute_class_options, st.session_state.get('general_compute_class')), key="general_compute_class")

                    dev_compute_class_options = database.get_compute_classes(dev_allowed=True)
                    st.selectbox("Compute class Dev *", dev_compute_class_options, index=get_select_box_index(dev_compute_class_options, st.session_state.get('general_compute_class_dev')), key="general_compute_class_dev")

                    st.text_input("Cost center", value=st.session_state.get('general_cost_center', ''), key="general_cost_center")
                    st.text_input("Warning DL group *", value=st.session_state.get('general_warning_dl_group', ''), key="general_warning_dl_group")
                
                with col3:
                    st.text_input("Spark configs", value=st.session_state.get('general_spark_configs', ''), key="general_spark_configs")
                    
                    st.number_input("Min version", value=st.session_state.get('general_min_version', 0.0), key="general_min_version")
                    
                    st.number_input("Max version", value=st.session_state.get('general_max_version', 0.0), key="general_max_version")
                    
                    st.text_input("Inserted By", value=st.session_state.get('general_inserted_by', ''), key="general_inserted_by")
                    st.text_input("Updated By", value=st.session_state.get('general_updated_by', ''), key="general_updated_by")

            if current_layer == "L0":
                st.header("Source Configuration")
                st.info("Source, Source Schema, and Source Object Name for each table must also be unique.")
                
                st.number_input(
                    "Number of Source Tables",
                    min_value=1,
                    max_value=5,
                    value=st.session_state.get('l0_num_tables', 1),
                    key='num_tables'
                )
                
                for i in range(int(st.session_state.get('num_tables', 1))):
                    with st.expander(f"Table {i+1}", expanded=True):
                        with st.container(border=True):
                            st.subheader("Source Configuration")
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.text_input("Source *", value=st.session_state.get(f'source_{i}', ''), key=f"source_{i}")
                                st.text_input("Source object name *", value=st.session_state.get(f'source_obj_name_{i}', ''), key=f"source_obj_name_{i}")
                                storage_options = ["R", "C"]
                                st.selectbox("Storage type *", storage_options, index=get_select_box_index(storage_options, st.session_state.get(f'storage_type_{i}', "R")), key=f"storage_type_{i}")
                            with col2:
                                st.text_input("Source schema *", value=st.session_state.get(f'source_schema_{i}', ''), key=f"source_schema_{i}")
                                st.text_input("Input file format *", value=st.session_state.get(f'input_file_format_{i}', ''), key=f"input_file_format_{i}")
                            with col3:
                                st.text_input("Delimeter", value=st.session_state.get(f'delimeter_{i}', ''), key=f"delimeter_{i}")
                                st.text_area("Custom schema", value=st.session_state.get(f'custom_schema_{i}', ''), key=f"custom_schema_{i}")

                        with st.container(border=True):
                            st.subheader("Data Quality & Processing")
                            st.text_area("DQ Logic *", value=st.session_state.get(f'dq_logic_{i}', ''), key=f"dq_logic_{i}")
                            st.text_area("CDC Logic *", value=st.session_state.get(f'cdc_logic_{i}', ''), key=f"cdc_logic_{i}")
                            st.text_area("Transform query *", value=st.session_state.get(f'transform_query_{i}', ''), key=f"transform_query_{i}")
                        
                        with st.container(border=True):
                            st.subheader("Target Configuration")
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                load_type_options = ["FULL", "DELTA"]
                                st.selectbox("Load type *", load_type_options, index=get_select_box_index(load_type_options, st.session_state.get(f'load_type_{i}', "FULL")), key=f"load_type_{i}")
                                prestag_options = ["Y", "N"]
                                st.selectbox("Prestag flag *", prestag_options, index=get_select_box_index(prestag_options, st.session_state.get(f'prestag_flag_{i}', "Y")), key=f"prestag_flag_{i}")
                            with col2:
                                st.text_input("LOB", value=st.session_state.get(f'lob_{i}', ''), key=f"lob_{i}")
                                st.text_input("Partition", value=st.session_state.get(f'partition_{i}', ''), key=f"partition_{i}")
                                status_options = ["Y", "N"]
                                st.selectbox("Status *", status_options, index=get_select_box_index(status_options, st.session_state.get(f'status_{i}', "Y")), key=f"status_{i}")
                            with col3:
                                st.subheader("Lift & Shift")
                                ls_flag_options = ["Y", "N"]
                                st.selectbox("LS Flag", ls_flag_options, index=get_select_box_index(ls_flag_options, st.session_state.get(f'ls_flag_{i}', "N")), key=f"ls_flag_{i}")
                                st.text_area("LS Detail", value=st.session_state.get(f'ls_detail_{i}', ''), key=f"ls_detail_{i}")
            
            if current_layer in ["L1", "L2"]:
                with st.container(border=True):
                    st.subheader("Target configuration")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        target_obj_options = ["Table", "MV", "Spark"]
                        st.selectbox("Target object type *", target_obj_options, index=get_select_box_index(target_obj_options, st.session_state.get('target_obj_type_L1_L2', 'Table')), key="target_obj_type_L1_L2")
                        st.text_input("LOB *", value=st.session_state.get('lob_L1_L2', ''), key="lob_L1_L2")
                        st.text_input("Source PK", value=st.session_state.get('source_pk_L1_L2', ''), key="source_pk_L1_L2")
                    with col2:
                        st.text_input("Target schema *", value=st.session_state.get('target_obj_schema_L1_L2', ''), key="target_obj_schema_L1_L2")
                        priority_options = ["Low", "Medium", "High", "Critical"]
                        st.selectbox("Priority *", priority_options, index=get_select_box_index(priority_options, st.session_state.get('priority_L1_L2', 'Medium')), key="priority_L1_L2")
                        st.text_area("Transform query *", value=st.session_state.get('transform_query_L1_L2', ''), key="transform_query_L1_L2")
                    with col3:
                        st.text_input("Target name *", value=st.session_state.get('target_obj_name_L1_L2', ''), key="target_obj_name_L1_L2")
                        st.text_input("Generic scripts", value=st.session_state.get('generic_scripts_L1_L2', ''), key="generic_scripts_L1_L2")
                        st.text_input("Target PK", value=st.session_state.get('target_pk_L1_L2', ''), key="target_pk_L1_L2")

                with st.container(border=True):
                    st.subheader("Data Management")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        load_type_options = ["FULL", "DELTA", "SCD"]
                        st.selectbox("Load type *", load_type_options, index=get_select_box_index(load_type_options, st.session_state.get('load_type_L1_L2', 'FULL')), key="load_type_L1_L2")
                        partition_method_options = ["Partition", "Liquid cluster"]
                        st.selectbox("Partition method", partition_method_options, index=get_select_box_index(partition_method_options, st.session_state.get('partition_method_L1_L2', 'Partition')), key="partition_method_L1_L2")
                    with col2:
                        st.text_input("Partition or Index", value=st.session_state.get('partition_or_index_L1_L2', ''), key="partition_or_index_L1_L2")
                        st.text_input("Custom script parameters", value=st.session_state.get('custom_script_params_L1_L2', ''), key="custom_script_params_L1_L2")
                    with col3:
                        st.text_input("Retention details *", value=st.session_state.get('retention_details_L1_L2', ''), key="retention_details_L1_L2")
                        status_options = ["Y", "N"]
                        st.selectbox("Status *", status_options, index=get_select_box_index(status_options, st.session_state.get('status_L1_L2', 'Y')), key="status_L1_L2")
            
            submitted = st.form_submit_button("Save All Data", type="primary")

            if submitted:
                general_data = {
                    "DATA_FLOW_GROUP_ID": st.session_state.general_data_flow_id,
                    "BUSINESS_UNIT": st.session_state.general_business_unit,
                    "PRODUCT_OWNER": st.session_state.general_product_owner,
                    "IS_ACTIVE": st.session_state.general_is_active,
                    "TRIGGER_TYPE": st.session_state.general_trigger_type,
                    "ETL_LAYER": st.session_state.general_etl_layer,
                    "DATA_SME": st.session_state.general_data_sme,
                    "INGESTION_MODE": st.session_state.general_ingestion_mode,
                    "INGESTION_BUCKET": st.session_state.general_ingestion_bucket,
                    "WARNING_THRESHOLD_MINS": st.session_state.general_warning_threshold,
                    "BUSINESS_OBJECT_NAME": st.session_state.general_business_object_name,
                    "COMPUTE_CLASS": st.session_state.general_compute_class,
                    "COMPUTE_CLASS_DEV": st.session_state.general_compute_class_dev,
                    "COST_CENTER": st.session_state.general_cost_center,
                    "WARNING_DL_GROUP": st.session_state.general_warning_dl_group,
                    "SPARK_CONFIGS": st.session_state.general_spark_configs,
                    "min_version": st.session_state.general_min_version,
                    "max_version": st.session_state.general_max_version,
                    "INSERTED_BY": st.session_state.general_inserted_by,
                    "UPDATED_BY": st.session_state.general_updated_by,
                }
                
                l0_tables_data_list = []
                if current_layer == "L0":
                    for i in range(int(st.session_state.num_tables)):
                        table_data = {
                            "l0_id": st.session_state.get(f'l0_id_{i}'),
                            "LOB": st.session_state[f"lob_{i}"],
                            "SOURCE": st.session_state[f"source_{i}"],
                            "SOURCE_OBJ_SCHEMA": st.session_state[f"source_schema_{i}"],
                            "SOURCE_OBJ_NAME": st.session_state[f"source_obj_name_{i}"],
                            "INPUT_FILE_FORMAT": st.session_state[f"input_file_format_{i}"],
                            "STORAGE_TYPE": st.session_state[f"storage_type_{i}"],
                            "CUSTOM_SCHEMA": st.session_state[f"custom_schema_{i}"],
                            "DELIMETER": st.session_state[f"delimeter_{i}"],
                            "DQ_LOGIC": st.session_state[f"dq_logic_{i}"],
                            "CDC_LOGIC": st.session_state[f"cdc_logic_{i}"],
                            "TRANSFORM_QUERY": st.session_state[f"transform_query_{i}"],
                            "LOAD_TYPE": st.session_state[f"load_type_{i}"],
                            "PRESTAG_FLAG": st.session_state[f"prestag_flag_{i}"],
                            "PARTITION": st.session_state[f"partition_{i}"],
                            "LS_FLAG": st.session_state[f"ls_flag_{i}"],
                            "LS_DETAIL": st.session_state[f"ls_detail_{i}"],
                            "IS_ACTIVE": st.session_state[f"status_{i}"],
                        }
                        l0_tables_data_list.append(table_data)

                form_data = {}
                if current_layer in ["L1", "L2"]:
                    form_data = {
                        "pb_id": st.session_state.get('pb_id'),
                        "TARGET_OBJ_TYPE": st.session_state.target_obj_type_L1_L2,
                        "LOB": st.session_state.lob_L1_L2,
                        "SOURCE_PK": st.session_state.source_pk_L1_L2,
                        "TARGET_OBJ_SCHEMA": st.session_state.target_obj_schema_L1_L2,
                        "PRIORITY": st.session_state.priority_L1_L2,
                        "TRANSFORM_QUERY": st.session_state.transform_query_L1_L2,
                        "TARGET_OBJ_NAME": st.session_state.target_obj_name_L1_L2,
                        "GENERIC_SCRIPTS": st.session_state.generic_scripts_L1_L2,
                        "TARGET_PK": st.session_state.target_pk_L1_L2,
                        "LOAD_TYPE": st.session_state.load_type_L1_L2,
                        "PARTITION_METHOD": st.session_state.partition_method_L1_L2,
                        "PARTITION_OR_INDEX": st.session_state.partition_or_index_L1_L2,
                        "CUSTOM_SCRIPT_PARAMS": st.session_state.custom_script_params_L1_L2,
                        "RETENTION_DETAILS": st.session_state.retention_details_L1_L2,
                        "IS_ACTIVE": st.session_state.status_L1_L2,
                    }
                
                is_valid = True
                required_general_fields = ["DATA_FLOW_GROUP_ID", "TRIGGER_TYPE", "ETL_LAYER", "COMPUTE_CLASS", "COMPUTE_CLASS_DEV", "DATA_SME", "PRODUCT_OWNER", "WARNING_THRESHOLD_MINS", "WARNING_DL_GROUP"]
                for field in required_general_fields:
                    if not general_data.get(field):
                        st.error(f"Please fill in all required General fields marked with an asterisk (*). Missing field: '{field}'")
                        is_valid = False
                        break
                
                if is_valid and current_layer == "L0":
                    if not general_data.get("INGESTION_MODE"):
                        st.error("Please fill in the required field: 'INGESTION_MODE'")
                        is_valid = False
                    
                    required_l0_fields = ["LOB", "SOURCE", "SOURCE_OBJ_SCHEMA", "SOURCE_OBJ_NAME", "INPUT_FILE_FORMAT", "STORAGE_TYPE", "DQ_LOGIC", "CDC_LOGIC", "TRANSFORM_QUERY", "LOAD_TYPE", "PRESTAG_FLAG", "IS_ACTIVE"]
                    for table_data in l0_tables_data_list:
                        for field in required_l0_fields:
                            if not table_data.get(field):
                                st.error(f"Please fill in all required fields for all tables. Missing field: {field}")
                                is_valid = False
                                break
                        if not is_valid: break
                
                if is_valid and current_layer in ["L1", "L2"]:
                    required_pb_fields = ["LOB", "TARGET_OBJ_SCHEMA", "PRIORITY", "TARGET_OBJ_TYPE", "TARGET_OBJ_NAME", "TRANSFORM_QUERY", "LOAD_TYPE", "RETENTION_DETAILS", "IS_ACTIVE"]
                    for field in required_pb_fields:
                        if not form_data.get(field):
                            st.error(f"Please fill in all required fields for L{current_layer}. Missing field: '{field}'")
                            is_valid = False
                            break

                if is_valid:
                    try:
                        data_for_db = general_data.copy()
                        data_for_db.pop('layer', None)
                        database.save_general_info(data_for_db)
                        if current_layer == "L0":
                            database.save_l0_details(l0_tables_data_list, data_for_db['DATA_FLOW_GROUP_ID'])
                        elif current_layer in ["L1", "L2"]:
                            database.save_pb_details(form_data, data_for_db['DATA_FLOW_GROUP_ID'])
                        st.success("Pipeline data saved successfully! ✅")
                        st.session_state.form_visible = False
                        st.session_state.edit_pipeline_id = None
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to save data. Please check logs for details. Error: {e}")

    else:
        st.subheader("Quick Actions")
        st.write("Start creating a new pipeline or choose from templates")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.container(border=True, height=120).markdown(f"""### Raw (L0) Layer\nRaw layer...""")
            if st.button("Create Raw Pipeline", key="create_raw", use_container_width=True):
                st.session_state.edit_pipeline_id = None
                st.session_state.current_pipeline_layer = "L0"
                st.session_state.form_visible = True
                st.rerun()
        with col2:
            st.container(border=True, height=120).markdown(f"""### Curated (L1) Layer\nCurated layer...""")
            if st.button("Create Curated Pipeline", key="create_curated", use_container_width=True):
                st.session_state.edit_pipeline_id = None
                st.session_state.current_pipeline_layer = "L1"
                st.session_state.form_visible = True
                st.rerun()
        with col3:
            st.container(border=True, height=120).markdown(f"""### Data Product (L2) Layer\nData Product layer...""")
            if st.button("Create Data Product Pipeline", key="create_data_product", use_container_width=True):
                st.session_state.edit_pipeline_id = None
                st.session_state.current_pipeline_layer = "L2"
                st.session_state.form_visible = True
                st.rerun()
        st.subheader("Recent Activity")
        st.write("Recently modified pipelines across all layers")
        pipelines = database.get_all_pipelines()
        if not pipelines:
            st.info("No pipelines found. Create one to get started!")
        else:
            for p in pipelines:
                col1, col2 = st.columns([4, 1])
                with col1:
                    st.markdown(f"**{p.get('DATA_FLOW_GROUP_ID', 'N/A')}**")
                    st.markdown(f"*{p.get('BUSINESS_UNIT', 'N/A')}* • Updated {p.get('UPDATED_TS', 'N/A')}")
                with col2:
                    if st.button("Edit", key=f"edit_{p.get('DATA_FLOW_GROUP_ID')}"):
                        st.session_state.edit_pipeline_id = p.get('DATA_FLOW_GROUP_ID')
                        st.session_state.current_pipeline_layer = p['ETL_LAYER']
                        st.session_state.form_visible = True
                        st.rerun()
                st.markdown("---")