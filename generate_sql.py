
import sqlite3
import uuid
import datetime

# --- Your Hardcoded Variables ---
# Use a new unique ID for each run to avoid primary key conflicts
DATA_FLOW_GROUP_ID = f'example_pipeline_{str(uuid.uuid4().hex[:8])}'
BUSINESS_UNIT = 'Marketing'
PRODUCT_OWNER = 'john.doe@company.com'
TRIGGER_TYPE = 'JOB'
BUSINESS_OBJECT_NAME = 'customer_data'
ETL_LAYER = 'L0' # Change to L0, L1, or L2
COMPUTE_CLASS = 'Large'
COMPUTE_CLASS_DEV = 'Medium'
DATA_SME = 'Data Team'
INGESTION_MODE = 'batch'
INGESTION_BUCKET = 's3://example-bucket/raw/'
SPARK_CONFIGS = '{"key": "value"}'
COST_CENTER = 'CC123'
WARNING_THRESHOLD_MINS = 60
WARNING_DL_GROUP = 'alerts@company.com'
min_version = 1.0
max_version = 2.0
IS_ACTIVE = 'N'
INSERTED_BY = 'script'
UPDATED_BY = 'script'

# --- Layer-specific variables (L0) as a list of tables ---
l0_tables_data = [
    {
        "LOB": "Consumer",
        "SOURCE": "Source_System_A",
        "SOURCE_OBJ_SCHEMA": "public",
        "SOURCE_OBJ_NAME": "customers",
        "LOAD_TYPE": "FULL",
        "INPUT_FILE_FORMAT": "csv",
        "STORAGE_TYPE": "R",
        "DQ_LOGIC": "NOT NULL on ID",
        "DELIMETER": ",",
        "CUSTOM_SCHEMA": "id int, name string",
        "CDC_LOGIC": "full refresh",
        "TRANSFORM_QUERY": "SELECT * FROM raw_customers",
        "PRESTAG_FLAG": "Y",
        "PARTITION": "load_date",
        "IS_ACTIVE": "Y",
        "LS_FLAG": "N",
        "LS_DETAIL": ""
    },
    {
        "LOB": "Consumer",
        "SOURCE": "Source_System_B",
        "SOURCE_OBJ_SCHEMA": "public",
        "SOURCE_OBJ_NAME": "orders",
        "LOAD_TYPE": "DELTA",
        "INPUT_FILE_FORMAT": "json",
        "STORAGE_TYPE": "R",
        "DQ_LOGIC": "NOT NULL on order_id",
        "DELIMETER": "",
        "CUSTOM_SCHEMA": "order_id int, item string",
        "CDC_LOGIC": "append only",
        "TRANSFORM_QUERY": "SELECT * FROM raw_orders",
        "PRESTAG_FLAG": "Y",
        "PARTITION": "order_date",
        "IS_ACTIVE": "Y",
        "LS_FLAG": "N",
        "LS_DETAIL": ""
    }
]

# --- Layer-specific variables (L1/L2) ---
pb_data = {
    "LOB": "Consumer",
    "TARGET_OBJ_SCHEMA": "curated",
    "TARGET_OBJ_NAME": "curated_customers",
    "PRIORITY": "2",
    "TARGET_OBJ_TYPE": "Table",
    "TRANSFORM_QUERY": "SELECT col1, col2 FROM raw_data",
    "GENERIC_SCRIPTS": "script_v1.py",
    "SOURCE_PK": "source_id",
    "TARGET_PK": "target_id",
    "LOAD_TYPE": "SCD",
    "PARTITION_METHOD": "Partition",
    "PARTITION_OR_INDEX": "partition_by_date",
    "CUSTOM_SCRIPT_PARAMS": "",
    "RETENTION_DETAILS": "90_days",
    "IS_ACTIVE": "Y"

}

# --- Database Interaction ---
if __name__ == '__main__':
    conn = None
    try:
        conn = sqlite3.connect('pipelines.db')
        cursor = conn.cursor()

        # Step 1: Insert into the header table using a parameterized query
        header_columns = [
            "DATA_FLOW_GROUP_ID", "BUSINESS_UNIT", "PRODUCT_OWNER", "TRIGGER_TYPE", 
            "BUSINESS_OBJECT_NAME", "ETL_LAYER", "COMPUTE_CLASS", "COMPUTE_CLASS_DEV", 
            "DATA_SME", "INGESTION_MODE", "INGESTION_BUCKET", "SPARK_CONFIGS", 
            "COST_CENTER", "WARNING_THRESHOLD_MINS", "WARNING_DL_GROUP", 
            "min_version", "max_version", "IS_ACTIVE", "INSERTED_BY", 
            "UPDATED_BY", "INSERTED_TS", "UPDATED_TS"
        ]
        header_values = (
            DATA_FLOW_GROUP_ID, BUSINESS_UNIT, PRODUCT_OWNER, TRIGGER_TYPE,
            BUSINESS_OBJECT_NAME, ETL_LAYER, COMPUTE_CLASS, COMPUTE_CLASS_DEV,
            DATA_SME, INGESTION_MODE, INGESTION_BUCKET, SPARK_CONFIGS,
            COST_CENTER, WARNING_THRESHOLD_MINS, WARNING_DL_GROUP,
            min_version, max_version, IS_ACTIVE, INSERTED_BY, UPDATED_BY,
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        header_query = f"INSERT INTO data_flow_control_header ({', '.join(header_columns)}) VALUES ({', '.join(['?'] * len(header_columns))})"
        
        print("--- Executing header query ---")
        cursor.execute(header_query, header_values)

        # Step 2: Insert into the layer-specific detail table(s)
        if ETL_LAYER == 'L0':
            for i, l0_data in enumerate(l0_tables_data):
                l0_data["DATA_FLOW_GROUP_ID"] = DATA_FLOW_GROUP_ID
                l0_columns = list(l0_data.keys())
                l0_values = tuple(l0_data.values())
                detail_query = f"INSERT INTO data_flow_l0_detail ({', '.join(l0_columns)}) VALUES ({', '.join(['?'] * len(l0_columns))})"
                
                print(f"\n--- Executing detail query for L0 table {i+1} ---")
                cursor.execute(detail_query, l0_values)
        elif ETL_LAYER in ['L1', 'L2']:
            pb_data["DATA_FLOW_GROUP_ID"] = DATA_FLOW_GROUP_ID
            pb_columns = list(pb_data.keys())
            pb_values = tuple(pb_data.values())
            detail_query = f"INSERT INTO data_flow_pb_detail ({', '.join(pb_columns)}) VALUES ({', '.join(['?'] * len(pb_columns))})"
            
            print("\n--- Executing detail query for L1/L2 table ---")
            cursor.execute(detail_query, pb_values)

        # Commit the transaction to save changes
        conn.commit()
        print("\nData successfully saved to database! ✅")
        
    except sqlite3.Error as e:
        print(f"\nDatabase error occurred: {e}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")