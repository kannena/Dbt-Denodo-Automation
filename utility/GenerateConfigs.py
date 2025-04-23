import snowflake.connector
import json
import yaml
import pandas as pd
import os
from collections import OrderedDict

# Load Snowflake connection details from config.json
with open('config.json', 'r') as f:
    config = json.load(f)

SRC_TABLE_NAME = config['Source_Table_name']
UserId = config['SnowFlakeUserId']
SnowFlakeAccount = config['SnowFlakeAccount']
SnowFlakewarehouse = config['SnowFlakewarehouse']
SnowFlakeRole = config['SnowFlakeRole']
SnowFlakeDatabase = config['SnowFlakeDatabase']
SnowFlakeSchema = config['SnowFlakeSchema']

# Connect to Snowflake
con = snowflake.connector.connect(
    user=f"{UserId}",
    authenticator="externalbrowser",
    account=f"{SnowFlakeAccount}",
    warehouse=f"{SnowFlakewarehouse}",
    role=f"{SnowFlakeRole}",
    database=f"{SnowFlakeDatabase}",
    schema=f"{SnowFlakeSchema}"
)

# Query Snowflake to get column metadata
sql = f"""
SELECT 
    ordinal_position, 
    column_name, 
    CASE 
        WHEN data_type = 'TEXT' THEN 'VARCHAR' 
        ELSE data_type 
    END AS data_type 
FROM information_schema.columns 
WHERE table_name = '{SRC_TABLE_NAME}' 
ORDER BY ordinal_position
"""
cursor = con.cursor()
cursor.execute(sql)
df = cursor.fetch_pandas_all()

# Generate YAML file
yaml_data = {
    "version": 2,
    "models": [
        {
            "name": SRC_TABLE_NAME,
            "description": f"{SRC_TABLE_NAME} table loaded from source system",
            "columns": [
                {
                    "name": row['COLUMN_NAME'],
                    "description": "",  # Empty description as a placeholder
                    "data_type": row['DATA_TYPE']
                }
                for _, row in df.iterrows()
            ]
        }
    ]
}

# Custom Dumper to ensure clean YAML output
class CleanDumper(yaml.Dumper):
    def increase_indent(self, flow=False, indentless=False):
        return super(CleanDumper, self).increase_indent(flow, False)

yaml_file_path = f"{SRC_TABLE_NAME}.yaml"
with open(yaml_file_path, 'w') as yaml_file:
    yaml.dump(yaml_data, yaml_file, Dumper=CleanDumper, default_flow_style=False, sort_keys=False)

print(f"✅ YAML file '{yaml_file_path}' has been created.")

# Generate JSON configuration file
config_template = {
    "SourceTable": SRC_TABLE_NAME,
    "TargetTable": SRC_TABLE_NAME,
    "KeyColumns": [df['COLUMN_NAME'][0]],  # Assuming the first column is the key
    "SourceApplicationName": "SOURCE_APP",  # Placeholder for source application name
    "MetirializationType": "incremental",
    "Tags": ["tag1", "tag2"],  # Placeholder tags
    "DenodoBaseViewPath": "denodo/views/base/",
    "DenodoCleanViewPath": "denodo/views/clean/",
    "DenodoBusinessEntityViewPath": "denodo/views/entity/",
    "Dbtmodelpath": "models/"
}

config_file_path = f"{SRC_TABLE_NAME}_config.json"
with open(config_file_path, 'w') as config_file:
    json.dump(config_template, config_file, indent=4)

print(f"✅ JSON configuration file '{config_file_path}' has been created.")

# Close Snowflake connection
con.close()