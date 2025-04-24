import snowflake.connector
import yaml
import json
import sys
import os

def update_column_descriptions(table_name):
    yaml_file = f"configs/{table_name}.yaml"
    config_file = f"configs/{table_name}.json"

    # Load YAML
    with open(yaml_file, 'r') as yml:
        column_data = yaml.safe_load(yml)
    print(f"column_data: {column_data}")

    # Load config for Snowflake credentials and table
    with open(config_file, 'r') as cfg:
        config = json.load(cfg)
    print(f"config: {config}")

    table_name = config['TargetTable']
    schema = config['SnowFlakeSchema']
    database = config['SnowFlakeDatabase']



    columns = column_data['models'][0]['columns']
    for column in columns:
        col_name = column['name']
        description = column.get('description', '')

        if description:
            sql = f"""ALTER TABLE "{database}"."{schema}"."{table_name}" MODIFY COLUMN "{col_name}" COMMENT = '{description}'"""
            print(f"Executing: {sql}")
            cur.execute(sql)


    print("✅ Descriptions updated successfully.")

if __name__ == "__main__":
    table_name = sys.argv[1]
    update_column_descriptions(table_name)
