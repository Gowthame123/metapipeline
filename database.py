
import sqlite3
import datetime

def init_db():
    """
    Initializes the SQLite database and creates the necessary tables.
    """
    conn = sqlite3.connect('pipelines.db')
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS data_flow_control_header (
            DATA_FLOW_GROUP_ID STRING PRIMARY KEY,
            BUSINESS_UNIT STRING,
            PRODUCT_OWNER STRING,
            TRIGGER_TYPE STRING,
            BUSINESS_OBJECT_NAME STRING,
            ETL_LAYER STRING,
            COMPUTE_CLASS STRING,
            COMPUTE_CLASS_DEV STRING,
            DATA_SME STRING,
            INGESTION_MODE STRING,
            INGESTION_BUCKET STRING,
            SPARK_CONFIGS STRING,
            COST_CENTER STRING,
            WARNING_THRESHOLD_MINS INT,
            WARNING_DL_GROUP STRING,
            min_version REAL,
            max_version REAL,
            IS_ACTIVE STRING,
            INSERTED_BY STRING,
            UPDATED_BY STRING,
            INSERTED_TS STRING,
            UPDATED_TS STRING
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS data_flow_l0_detail (
            l0_id INTEGER PRIMARY KEY AUTOINCREMENT,
            DATA_FLOW_GROUP_ID STRING,
            SOURCE STRING NOT NULL,
            SOURCE_OBJ_SCHEMA STRING NOT NULL,
            SOURCE_OBJ_NAME STRING NOT NULL,
            INPUT_FILE_FORMAT STRING,
            STORAGE_TYPE STRING,
            CUSTOM_SCHEMA STRING,
            DELIMETER STRING,
            DQ_LOGIC STRING,
            CDC_LOGIC STRING,
            TRANSFORM_QUERY STRING,
            LOAD_TYPE STRING,
            PRESTAG_FLAG STRING,
            PARTITION STRING,
            LS_FLAG STRING,
            LS_DETAIL STRING,
            LOB STRING,
            IS_ACTIVE STRING,
            INSERTED_BY STRING,
            UPDATED_BY STRING,       
            FOREIGN KEY (DATA_FLOW_GROUP_ID) REFERENCES data_flow_control_header(DATA_FLOW_GROUP_ID),
            UNIQUE (SOURCE, SOURCE_OBJ_SCHEMA, SOURCE_OBJ_NAME)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS data_flow_pb_detail (
            pb_id INTEGER PRIMARY KEY AUTOINCREMENT,
            DATA_FLOW_GROUP_ID STRING,
            TARGET_OBJ_SCHEMA STRING,
            TARGET_OBJ_NAME STRING,
            PRIORITY STRING,
            TARGET_OBJ_TYPE STRING,
            TRANSFORM_QUERY STRING,
            GENERIC_SCRIPTS STRING,
            SOURCE_PK STRING,
            TARGET_PK STRING,
            LOAD_TYPE STRING,
            PARTITION_METHOD STRING,
            PARTITION_OR_INDEX STRING,
            CUSTOM_SCRIPT_PARAMS STRING,
            RETENTION_DETAILS STRING,
            LOB STRING,
            IS_ACTIVE STRING,
            INSERTED_BY STRING,
            UPDATED_BY STRING,
            FOREIGN KEY (DATA_FLOW_GROUP_ID) REFERENCES data_flow_control_header(DATA_FLOW_GROUP_ID)
        )
    """)

    # NEW Table: data_flow_cluster_config_lookup
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS data_flow_cluster_config_lookup (
            COMPUTE_CLASS STRING PRIMARY KEY,
            DESCRIPTION STRING,
            MIN_WORKER INT,
            MAX_WORKER INT,
            DRIVER_NODE_TYPE_ID STRING,
            WORKER_NODE_TYPE_ID STRING,
            RUNTIME_ENGINE STRING,
            SPARK_VERSION STRING,
            INSERTED_BY STRING,
            UPDATED_BY STRING,
            INSERTED_TS TEXT,
            UPDATED_TS TEXT,
            DEV_ALLOWED STRING
        )
    """)

    conn.commit()
    conn.close()
    
    _seed_cluster_config_data()

def _seed_cluster_config_data():
    """
    Seeds the data_flow_cluster_config_lookup table with hardcoded data if it's empty.
    """
    conn = sqlite3.connect('pipelines.db')
    cursor = conn.cursor()

    # Check if the table is empty before inserting
    cursor.execute("SELECT COUNT(*) FROM data_flow_cluster_config_lookup")
    if cursor.fetchone()[0] > 0:
        conn.close()
        return

    # Data from the user's image
    data = [
        ('M_M6', 'M_M6', 2, 8, None, None, None, None, None, None, None, None, 'Y'),
        ('L_M6', 'L_M6', 4, 16, None, None, None, None, None, None, None, None, 'Y'),
        ('S_M6', 'S_M6', 1, 4, None, None, None, None, None, None, None, None, 'Y'),
        ('XL_M6', 'XL_M6', 8, 32, None, None, None, None, None, None, None, None, 'N'),
        ('S_I3', 'S_I3', 1, 4, None, None, 'x', None, None, None, None, None, 'Y'),
        ('M_I3', 'M_I3', 2, 8, None, None, None, None, None, None, None, None, 'Y'),
        ('L_I3', 'L_I3', 4, 16, None, None, None, None, None, None, None, None, 'Y'),
        ('XL_I3', 'XL_I3', 8, 32, None, None, None, None, None, None, None, None, 'N'),
        ('S_R5', 'S_R5', 1, 4, None, None, None, None, None, None, None, None, 'Y'),
        ('M_R5', 'M_R5', 2, 8, None, None, None, None, None, None, None, None, 'Y'),
        ('L_R5', 'L_R5', 4, 16, None, None, None, None, None, None, None, None, 'N'),
        ('XL_R5', 'XL_R5', 8, 32, None, None, None, None, None, None, None, None, 'N'),
        ('S_C5', 'S_C5', 1, 4, None, None, None, None, None, None, None, None, 'Y'),
        ('M_C5', 'M_C5', 2, 8, None, None, None, None, None, None, None, None, 'Y'),
        ('L_C5', 'L_C5', 4, 16, None, None, None, None, None, None, None, None, 'Y'),
        ('XL_C5', 'XL_C5', 8, 32, None, None, None, None, None, None, None, None, 'N'),
        ('XXL_C5', 'XXL_C5', 16, 64, None, None, None, None, None, None, None, None, 'N'),
        ('Serverless', 'Serverless', None, None, None, None, None, None, None, None, None, None, 'Y'),
        ('S_R5_WP', 'S_R5_WP', 1, 4, None, None, None, None, None, None, None, None, 'N'),
        ('M_R5_WP', 'M_R5_WP', 2, 8, None, None, None, None, None, None, None, None, 'N'),
        ('L_R5_WP', 'L_R5_WP', 4, 16, None, None, None, None, None, None, None, None, 'N'),
        ('XL_R5_WP', 'XL_R5_WP', 8, 32, None, None, None, None, None, None, None, None, 'N'),
        ('XXL_R5_WP', 'XXL_R5_WP', 16, 64, None, None, None, None, None, None, None, None, 'N'),
        ('S_C6_WP', 'S_C6_WP', 1, 4, None, None, None, None, None, None, None, None, 'N'),
        ('M_C6_WP', 'M_C6_WP', 2, 8, None, None, None, None, None, None, None, None, 'N'),
        ('L_C6_WP', 'L_C6_WP', 4, 16, None, None, None, None, None, None, None, None, 'N'),
        ('XL_C6_WP', 'XL_C6_WP', 8, 32, None, None, None, None, None, None, None, None, 'N'),
        ('XXL_C6_WP', 'XXL_C6_WP', 16, 64, None, None, None, None, None, None, None, None, 'N')
    ]

    # Insert the data into the table
    cursor.executemany("""
        INSERT INTO data_flow_cluster_config_lookup (
            COMPUTE_CLASS, DESCRIPTION, MIN_WORKER, MAX_WORKER, DRIVER_NODE_TYPE_ID,
            WORKER_NODE_TYPE_ID, RUNTIME_ENGINE, SPARK_VERSION, INSERTED_BY, UPDATED_BY,
            INSERTED_TS, UPDATED_TS, DEV_ALLOWED
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, data)

    conn.commit()
    conn.close()

def save_general_info(general_data):
    """Inserts or updates the header record based on DATA_FLOW_GROUP_ID."""
    conn = sqlite3.connect('pipelines.db')
    cursor = conn.cursor()
    
    data_flow_group_id = general_data.get('DATA_FLOW_GROUP_ID')
    general_data['UPDATED_TS'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")    
    expected_columns = [
        "DATA_FLOW_GROUP_ID", "BUSINESS_UNIT", "PRODUCT_OWNER", "TRIGGER_TYPE", 
        "BUSINESS_OBJECT_NAME", "ETL_LAYER", "COMPUTE_CLASS", "COMPUTE_CLASS_DEV", 
        "DATA_SME", "INGESTION_MODE", "INGESTION_BUCKET", "SPARK_CONFIGS", 
        "COST_CENTER", "WARNING_THRESHOLD_MINS", "WARNING_DL_GROUP", 
        "min_version", "max_version", "IS_ACTIVE", "INSERTED_BY", 
        "UPDATED_BY", "INSERTED_TS", "UPDATED_TS"
    ]
    
    data_for_db = {key: general_data[key] for key in expected_columns if key in general_data}

    cursor.execute("SELECT COUNT(*) FROM data_flow_control_header WHERE DATA_FLOW_GROUP_ID = ?", (data_flow_group_id,))
    exists = cursor.fetchone()[0]
    
    columns = ', '.join(data_for_db.keys())
    placeholders = ', '.join('?' * len(data_for_db))
    values = tuple(data_for_db.values())
    
    if exists > 0:
        update_cols = ', '.join([f'{col} = ?' for col in data_for_db.keys()])
        cursor.execute(f"UPDATE data_flow_control_header SET {update_cols} WHERE DATA_FLOW_GROUP_ID = ?", values + (data_flow_group_id,))
    else:
        general_data['INSERTED_TS'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute(f"INSERT INTO data_flow_control_header ({columns}) VALUES ({placeholders})", values)
    
    conn.commit()
    conn.close()


def save_l0_details(l0_data_list, data_flow_group_id):
    """
    Saves a list of L0 detail records for a given pipeline header.
    It performs inserts for new records and updates for existing ones.
    """
    conn = sqlite3.connect('pipelines.db')
    cursor = conn.cursor()

    for l0_data in l0_data_list:
        l0_data['DATA_FLOW_GROUP_ID'] = data_flow_group_id
        
        # Check if the record already exists using its unique composite key
        cursor.execute("""
            SELECT COUNT(*) FROM data_flow_l0_detail 
            WHERE DATA_FLOW_GROUP_ID = ? AND SOURCE = ? AND SOURCE_OBJ_SCHEMA = ? AND SOURCE_OBJ_NAME = ?
        """, (data_flow_group_id, l0_data['SOURCE'], l0_data['SOURCE_OBJ_SCHEMA'], l0_data['SOURCE_OBJ_NAME']))
        exists = cursor.fetchone()[0]

        if exists > 0:
            # Update existing record
            l0_data.pop('DATA_FLOW_GROUP_ID') # Don't update the foreign key
            columns_to_update = [key for key in l0_data.keys()]
            values_to_update = list(l0_data.values())
            update_cols_str = ', '.join([f'{col} = ?' for col in columns_to_update])
            cursor.execute(f"UPDATE data_flow_l0_detail SET {update_cols_str} WHERE DATA_FLOW_GROUP_ID = ? AND SOURCE = ? AND SOURCE_OBJ_SCHEMA = ? AND SOURCE_OBJ_NAME = ?", values_to_update + [data_flow_group_id, l0_data['SOURCE'], l0_data['SOURCE_OBJ_SCHEMA'], l0_data['SOURCE_OBJ_NAME']])
        else:
            # Insert new record
            columns = ', '.join(l0_data.keys())
            placeholders = ', '.join('?' * len(l0_data))
            values = tuple(l0_data.values())
            cursor.execute(f"INSERT INTO data_flow_l0_detail ({columns}) VALUES ({placeholders})", values)
    
    conn.commit()
    conn.close()


def save_pb_details(pb_data, data_flow_group_id):
    """Saves a single L1/L2 detail record for a given pipeline header."""
    conn = sqlite3.connect('pipelines.db')
    cursor = conn.cursor()

    cursor.execute("DELETE FROM data_flow_pb_detail WHERE DATA_FLOW_GROUP_ID = ?", (data_flow_group_id,))

    pb_data['DATA_FLOW_GROUP_ID'] = data_flow_group_id
    columns = ', '.join(pb_data.keys())
    placeholders = ', '.join('?' * len(pb_data))
    values = tuple(pb_data.values())
    cursor.execute(f"INSERT INTO data_flow_pb_detail ({columns}) VALUES ({placeholders})", values)
    
    conn.commit()
    conn.close()

def get_all_pipelines():
    """
    Fetches all pipelines by selecting only from the header table.
    """
    conn = sqlite3.connect('pipelines.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM data_flow_control_header")
    headers = [dict(zip([col[0] for col in cursor.description], row)) for row in cursor.fetchall()]
    conn.close()
    return headers

def get_pipeline_by_id(data_flow_group_id):
    """Fetches a single pipeline and its detail records by ID."""
    conn = sqlite3.connect('pipelines.db')
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM data_flow_control_header WHERE DATA_FLOW_GROUP_ID = ?", (data_flow_group_id,))
    columns = [col[0] for col in cursor.description]
    pipeline_data = cursor.fetchone()

    if not pipeline_data:
        conn.close()
        return None

    pipeline_dict = dict(zip(columns, pipeline_data))

    cursor.execute("SELECT * FROM data_flow_l0_detail WHERE DATA_FLOW_GROUP_ID = ?", (data_flow_group_id,))
    l0_columns = [col[0] for col in cursor.description]
    l0_details = [dict(zip(l0_columns, row)) for row in cursor.fetchall()]
    pipeline_dict['l0_details'] = l0_details

    cursor.execute("SELECT * FROM data_flow_pb_detail WHERE DATA_FLOW_GROUP_ID = ?", (data_flow_group_id,))
    pb_columns = [col[0] for col in cursor.description]
    pb_details = [dict(zip(pb_columns, row)) for row in cursor.fetchall()]
    pipeline_dict['pb_details'] = pb_details
    
    conn.close()
    return pipeline_dict

def delete_pipeline(data_flow_group_id):
    """Deletes a complete pipeline and all its associated records."""
    conn = sqlite3.connect('pipelines.db')
    cursor = conn.cursor()
    
    try:
        cursor.execute("DELETE FROM data_flow_l0_detail WHERE DATA_FLOW_GROUP_ID = ?", (data_flow_group_id,))
        cursor.execute("DELETE FROM data_flow_pb_detail WHERE DATA_FLOW_GROUP_ID = ?", (data_flow_group_id,))
        cursor.execute("DELETE FROM data_flow_control_header WHERE DATA_FLOW_GROUP_ID = ?", (data_flow_group_id,))
        conn.commit()
        return True
    except sqlite3.Error as e:
        print(f"Error deleting pipeline: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def delete_l0_detail(l0_id):
    """Deletes a single L0 source table record."""
    conn = sqlite3.connect('pipelines.db')
    cursor = conn.cursor()
    
    try:
        cursor.execute("DELETE FROM data_flow_l0_detail WHERE l0_id = ?", (l0_id,))
        conn.commit()
        return True
    except sqlite3.Error as e:
        print(f"Error deleting L0 detail: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def get_compute_classes(dev_allowed=False):
    """
    Fetches COMPUTE_CLASS options from the lookup table.
    
    Args:
        dev_allowed (bool): If True, fetches only classes where DEV_ALLOWED is 'Y'.
    
    Returns:
        list: A sorted list of unique compute class names.
    """
    conn = sqlite3.connect('pipelines.db')
    cursor = conn.cursor()
    
    if dev_allowed:
        query = "SELECT COMPUTE_CLASS FROM data_flow_cluster_config_lookup WHERE DEV_ALLOWED = 'Y';"
    else:
        query = "SELECT COMPUTE_CLASS FROM data_flow_cluster_config_lookup;"
        
    cursor.execute(query)
    classes = [row[0] for row in cursor.fetchall() if row[0] is not None]
    conn.close()
    
    return sorted(list(set(classes)))

# import sqlite3
# import datetime

# This is the updated save_to_db function. You should replace your existing one with this.
def save_to_db(table_name, data):
    """Inserts or updates a record in the specified table, handling None values."""
    conn = sqlite3.connect('pipelines.db')
    cursor = conn.cursor()

    # --- NEW: Data Cleaning & Pre-processing ---
    data_to_save = data.copy()
    
    # Define fields that should be treated as numbers
    numeric_fields = ['WARNING_THRESHOLD_MINS', 'min_version', 'max_version', 'MIN_WORKER', 'MAX_WORKER']
    
    for key, value in data_to_save.items():
        if value is None:
            if key in numeric_fields:
                data_to_save[key] = 0.0  # Convert None to a default number
            else:
                data_to_save[key] = ''   # Convert None to an empty string

    data_to_save['INSERTED_TS'] = datetime.datetime.now().isoformat()
    data_to_save['UPDATED_TS'] = datetime.datetime.now().isoformat()
    # --- END NEW ---

    # Determine the unique key for each table
    unique_key_cols = None
    if table_name == 'data_flow_control_header':
        unique_key_cols = ['DATA_FLOW_GROUP_ID']
    elif table_name == 'data_flow_l0_detail':
        unique_key_cols = ['DATA_FLOW_GROUP_ID', 'SOURCE', 'SOURCE_OBJ_SCHEMA', 'SOURCE_OBJ_NAME']
    elif table_name == 'data_flow_pb_detail':
        unique_key_cols = ['DATA_FLOW_GROUP_ID']
    
    exists = False
    if unique_key_cols and all(col in data_to_save for col in unique_key_cols):
        placeholders = ' AND '.join([f'{col} = ?' for col in unique_key_cols])
        values = tuple(data_to_save[col] for col in unique_key_cols)
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {placeholders}", values)
            exists = cursor.fetchone()[0] > 0
        except sqlite3.OperationalError as e:
            print(f"Database error during check: {e}")
            conn.close()
            raise

    # Prepare columns and values for INSERT or UPDATE
    columns = ', '.join(data_to_save.keys())
    placeholders = ', '.join(['?'] * len(data_to_save))
    values = tuple(data_to_save.values())

    try:
        if exists:
            update_cols = ', '.join([f'{col} = ?' for col in data_to_save.keys() if col not in unique_key_cols and col != 'l0_id' and col != 'pb_id'])
            update_values_list = [data_to_save[key] for key in data_to_save.keys() if key not in unique_key_cols and key != 'l0_id' and key != 'pb_id']
            update_values_list.extend([data_to_save[col] for col in unique_key_cols])
            
            sql_query = f"UPDATE {table_name} SET {update_cols} WHERE {' AND '.join([f'{col} = ?' for col in unique_key_cols])}"
            
            print(f"Executing UPDATE: {sql_query}")
            print(f"With values: {tuple(update_values_list)}")
            cursor.execute(sql_query, tuple(update_values_list))
        else:
            sql_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            print(f"Executing INSERT: {sql_query}")
            print(f"With values: {values}")
            cursor.execute(sql_query, values)
        
        conn.commit()
        print("Database commit successful.")

    except sqlite3.IntegrityError as e:
        print(f"Integrity Error: {e}")
        conn.rollback()
        raise
    except sqlite3.OperationalError as e:
        print(f"Operational Error: {e}")
        conn.rollback()
        raise
    except Exception as e:
        print(f"An unexpected error occurred during database operation: {e}")
        conn.rollback()
        raise
        
    finally:
        conn.close()

# Note: You still need to fix the get_pipeline_by_id function to return clean data to your UI as discussed before.
# This fix prevents bad data from ever being saved, but your UI still needs to be prepared for it,
# so that the old data won't cause it to crash.