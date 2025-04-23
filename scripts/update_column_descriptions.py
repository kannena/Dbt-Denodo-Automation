import snowflake.connector
import yaml
import sys
import os

def update_column_descriptions(yaml_file, config_file):
    # Load YAML
    with open(yaml_file, 'r') as yml:
        column_data = yaml.safe_load(yml)

    # Load config for Snowflake credentials and table
    with open(config_file, 'r') as cfg:
        config = yaml.safe_load(cfg)
    print(f"config: {config}")
    table_name = config['TargetTable']
    schema = config['SnowFlakeSchema']
    database = config['SnowFlakeDatabase']


    for column in column_data['columns']:
        col_name = column['name']
        description = column.get('description', '')

        if description:
            sql = f"""ALTER TABLE "{database}"."{schema}"."{table_name}" MODIFY COLUMN "{col_name}" COMMENT = '{description}'"""
            print(f"Executing: {sql}")
            # cur.execute(sql)

    cur.close()
    conn.close()
    print("✅ Descriptions updated successfully.")

if __name__ == "__main__":
    yaml_file = sys.argv[1]
    config_file = sys.argv[2]
    update_column_descriptions(yaml_file, config_file)
