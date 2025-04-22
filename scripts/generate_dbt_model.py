import yaml
import json
import os

# Load config
with open('configs/template_config.json') as f:
    config = json.load(f)

model_path = config["Dbtmodelpath"]
target_table = config["TargetTable"]
source_table = config["SourceTable"]
source_app = config["SourceApplicationName"]
materialization_type = config["MetirializationType"]
tags = config["Tags"]
key_columns = config["KeyColumns"]

# Load YAML column definitions
yaml_file = f'configs/{target_table}.yaml'
with open(yaml_file, 'r') as f:
    content = yaml.safe_load(f)

columns = content['models'][0]['columns']
description = content['models'][0]['description']

# Generate SELECT block with transformations
select_lines = []
for col in columns:
    col_name = col["name"]
    col_comment = col["description"]
    if col_name == "SF_INSERT_TIMESTAMP":
        select_lines.append(f'  {col_name} AS {col_name} -- {col_comment}')
    elif col_name.endswith("_DATE"):
        select_lines.append(f'  {{ string_to_timezone_ntz(\'{col_name}\') }} AS {col_name} -- {col_comment}')
    elif col_name.endswith("_ID"):
        select_lines.append(f'  {{ string_to_number(\'{col_name}\', 38, 0) }} AS {col_name} -- {col_comment}')
    else:
        select_lines.append(f'  {{ set_varchar_length(\'{col_name}\', 240) }} AS {col_name} -- {col_comment}')

select_block = ",\n".join(select_lines)

# Generate final SQL
sql = f"""
{{
    config(
        materialized='{materialization_type}',
        unique_key='PK_{target_table}_ID', -- PK_(TARGET TABLE NAME)_ID
        merge_no_update_columns=['SYS_CREATE_DTM'],
        tags={tags}
    )
}}

-- {description}
WITH
GET_NEW_RECORDS AS (
  SELECT *, 1 AS BATCH_KEY_ID
  FROM
  {{% raw %}}{{{{ source('{source_app}', '{source_table}') }}}}{{% endraw %}}
  -- Get last update from staging table
  {{% if is_incremental() %}}
  WHERE
  SF_INSERT_TIMESTAMP > '{{{{ get_max_event_time('SF_INSERT_TIMESTAMP', not_minus3=True) }}}}'
  {{% endif %}}
),
-- Remove any duplicate data from multiple event triggers, partition will be on key columns
DEDUPE_CTE AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY {", ".join(key_columns)} ORDER BY SF_INSERT_TIMESTAMP DESC) AS ROW_NUM
  FROM GET_NEW_RECORDS
),
INS_BATCH_ID AS (
    SELECT TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISSFF3')) AS INS_BATCH_ID, 1 AS BATCH_KEY_ID
)

SELECT
  -- PK generation on key columns
  {{ generate_surrogate_key([{", ".join([f"'{col}'" for col in key_columns])}]) }} AS PK_{target_table}_ID,
  CURRENT_TIMESTAMP AS SYS_CREATE_DTM,
  CURRENT_TIMESTAMP AS SYS_LAST_UPDATE_DTM,
  INS_BATCH_ID AS SYS_EXEC_ID,
{select_block}
FROM DEDUPE_CTE
LEFT JOIN INS_BATCH_ID USING (BATCH_KEY_ID)
WHERE ROW_NUM = 1;
"""

# Ensure directory exists
os.makedirs(model_path, exist_ok=True)

# Write SQL
output_file = os.path.join(model_path, f'{target_table}.sql')
with open(output_file, 'w') as f:
    f.write(sql)

print(f"✅ dbt model created at {output_file}")
