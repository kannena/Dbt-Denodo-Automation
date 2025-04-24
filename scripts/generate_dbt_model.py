import sys
import json
import yaml
import os

table_name = sys.argv[1]

json_path = f'configs/{table_name}.json'
yaml_path = f'configs/{table_name}.yaml'

with open(json_path) as f:
    config = json.load(f)

with open(yaml_path) as f:
    data = yaml.safe_load(f)

columns = data['models'][0]['columns']
model_path = config["Dbtmodelpath"]
source_table = config["SourceTable"]
source_app = config["SourceApplicationName"]
materialization_type = config["MetirializationType"]
tags = config["Tags"]
key_columns = config["KeyColumns"]
description = data['models'][0].get('description', '')

# Generate SELECT lines
select_lines = []
for col in columns:
    col_name = col["name"]
    col_comment = col.get("description", "")
    if col_name == "SF_INSERT_TIMESTAMP":
        select_lines.append(f'  {col_name} AS {col_name} -- {col_comment}')
    elif col_name.endswith("_DATE"):
        macro = f'string_to_timezone_ntz("{col_name}")'
        select_lines.append(f'  {{{{ {macro} }}}} AS {col_name}, -- {col_comment}')
    elif col_name.endswith("_ID"):
        macro = f'string_to_number("{col_name}", 38, 0)'
        select_lines.append(f'  {{{{ {macro} }}}} AS {col_name}, -- {col_comment}')
    else:
        macro = f'set_varchar_length("{col_name}", 240)'
        select_lines.append(f'  {{{{ {macro} }}}} AS {col_name}, -- {col_comment}')

select_block = ",\n".join(select_lines)

# Start building SQL in parts
sql_parts = []

# Header with config
sql_parts.append(f"""{{{{ config(
    materialized='{materialization_type}',
    unique_key='PK_{table_name}_ID',
    merge_no_update_columns=['SYS_CREATE_DTM'],
    tags={tags}
) }}}}

-- {description}
WITH
GET_NEW_RECORDS AS (
  SELECT *, 1 AS BATCH_KEY_ID
  FROM
  {{{{ source('{source_app}', '{source_table}') }}}}
""")

# Append the Jinja block *outside* f-string
sql_parts.append("""
  {% if is_incremental() %}
  WHERE
  SF_INSERT_TIMESTAMP > '{{ get_max_event_time("SF_INSERT_TIMESTAMP", not_minus3=True) }}'
  {% endif %}
),
""")

# Continue SQL
sql_parts.append(f"""DEDUPE_CTE AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY {", ".join(key_columns)} ORDER BY SF_INSERT_TIMESTAMP DESC) AS ROW_NUM
  FROM GET_NEW_RECORDS
),
INS_BATCH_ID AS (
  SELECT TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISSFF3')) AS INS_BATCH_ID, 1 AS BATCH_KEY_ID
)

SELECT
  {{{{ generate_surrogate_key([{", ".join([f"'{col}'" for col in key_columns])}]) }}}} AS PK_{table_name}_ID,
  CURRENT_TIMESTAMP AS SYS_CREATE_DTM,
  CURRENT_TIMESTAMP AS SYS_LAST_UPDATE_DTM,
  INS_BATCH_ID AS SYS_EXEC_ID,
{select_block}
FROM DEDUPE_CTE
LEFT JOIN INS_BATCH_ID USING (BATCH_KEY_ID)
WHERE ROW_NUM = 1;
""")

# Final SQL string
final_sql = "".join(sql_parts)

# Write output
os.makedirs(model_path, exist_ok=True)
output_file = os.path.join(model_path, f'{table_name}.sql')
with open(output_file, 'w') as f:
    f.write(final_sql)

print(f"✅ dbt model created at {output_file}")
