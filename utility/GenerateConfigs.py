import snowflake.connector
import json
import yaml
import pandas as pd
import os

# Load Snowflake connection details from config.json
with open('config.json', 'r') as f:
    config = json.load(f)

SRC_TABLE_NAME = config['Source_Table_name']
TGT_TABLE_NAME = config['Target_Table_name']
UserId = config['SnowFlakeUserId']
SnowFlakeAccount = config['SnowFlakeAccount']
SnowFlakewarehouse = config['SnowFlakewarehouse']
SnowFlakeRole = config['SnowFlakeRole']
SnowFlakeDatabase = config['SnowFlakeDatabase']
SnowFlakeSchema = config['SnowFlakeSchema']

# Connect to Snowflake
con = snowflake.connector.connect(
    user=UserId,
    authenticator="externalbrowser",
    account=SnowFlakeAccount,
    warehouse=SnowFlakewarehouse,
    role=SnowFlakeRole,
    database=SnowFlakeDatabase,
    schema=SnowFlakeSchema
)

# Query column metadata
sql = f"""
SELECT DISTINCT
    ordinal_position, 
    column_name, 
    CASE 
        WHEN data_type = 'TEXT' THEN 'VARCHAR' 
        ELSE data_type 
    END AS data_type 
FROM information_schema.columns 
WHERE table_name = '{SRC_TABLE_NAME}' AND column_name NOT LIKE 'DTL__CI_%' AND column_name NOT LIKE 'DTL__BI_%'
ORDER BY ordinal_position
"""
cursor = con.cursor()
cursor.execute(sql)
df = cursor.fetch_pandas_all()

# Save the original first column for JSON config
first_row = df.iloc[0]
pk_column_name = first_row['COLUMN_NAME']

# Build source columns (excluding first since it's used for PK remapping)
source_columns = [
    {
        "name": row['COLUMN_NAME'],
        "description": "",
        "data_type": row['DATA_TYPE']
    }
    for i, row in df.iterrows() if i != 0
]

# Define the renamed PK column
pk_column = {
    "name": f"PK_{TGT_TABLE_NAME}_ID",
    "description": "",
    "data_type": first_row['DATA_TYPE']
}

# Define audit columns
audit_columns = [
    {"name": "SYS_CREATE_DTM", "description": "", "data_type": "TIMESTAMP_LTZ"},
    {"name": "SYS_EXEC_ID", "description": "", "data_type": "NUMBER"},
    {"name": "SYS_LAST_UPDATE_DTM", "description": "", "data_type": "TIMESTAMP_LTZ"},
    {"name": "SYS_ACTION_CD", "description": "", "data_type": "VARCHAR"},
    {"name": "SYS_DEL_IND", "description": "", "data_type": "VARCHAR"},
    {"name": "SYS_VALID_IND", "description": "", "data_type": "VARCHAR"},
    {"name": "SYS_INVALID_DESC", "description": "", "data_type": "VARCHAR"},
    {"name": "SYS_CDC_DTM", "description": "", "data_type": "TIMESTAMP_TZ"},
    {"name": "SYS_CDC_LIB", "description": "", "data_type": "VARCHAR"}
]

# YAML structure
yaml_data = {
    "version": 2,
    "models": [
        {
            "name": TGT_TABLE_NAME,
            "description": f"{TGT_TABLE_NAME} table loaded from source system",
            "columns": [pk_column] + audit_columns + source_columns
        }
    ]
}

# YAML formatting
class CleanDumper(yaml.Dumper):
    def increase_indent(self, flow=False, indentless=False):
        return super(CleanDumper, self).increase_indent(flow, False)

yaml_file_path = f"{TGT_TABLE_NAME}.yaml"
with open(yaml_file_path, 'w') as yaml_file:
    yaml.dump(yaml_data, yaml_file, Dumper=CleanDumper, default_flow_style=False, sort_keys=False)
print(f"✅ YAML file '{yaml_file_path}' has been created.")

# JSON config
json_config = {
    "SourceTable": SRC_TABLE_NAME,
    "TargetTable": TGT_TABLE_NAME,
    "KeyColumns": [pk_column_name],  # original source PK
    "SourceApplicationName": "SOURCE_APP",
    "MetirializationType": "incremental",
    "Tags": ["tag1", "tag2"],
    "DenodoBaseViewPath": "denodo/views/base/",
    "DenodoCleanViewPath": "denodo/views/clean/",
    "DenodoBusinessEntityViewPath": "denodo/views/entity/",
    "Dbtmodelpath": "models/"
}

json_file_path = f"{TGT_TABLE_NAME}.json"
with open(json_file_path, 'w') as json_file:
    json.dump(json_config, json_file, indent=4)
print(f"✅ JSON configuration file '{json_file_path}' has been created.")

# Close Snowflake connection
con.close()
