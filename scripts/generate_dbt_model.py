import yaml
import json
import os

# Load config
with open('configs/template_config.json') as f:
    config = json.load(f)

model_path = config["Dbtmodelpath"]
target_table = config["TargetTable"]

# Load YAML column definitions
yaml_file = f'configs/{target_table}.yaml'
with open(yaml_file, 'r') as f:
    content = yaml.safe_load(f)

columns = content['models'][0]['columns']
description = content['models'][0]['description']

# Build SELECT with comments
select_lines = []
for col in columns:
    col_name = col["name"]
    col_comment = col["description"]
    select_lines.append(f'    {col_name} -- {col_comment}')

sql = f"""{{{{ config(materialized='{config["MetirializationType"]}', tags={config["Tags"]}) }}}}

-- {description}
SELECT
{',\n'.join(select_lines)}
FROM {{% raw %}}{{{{ source('{config["SourceApplicationName"]}', '{config["SourceTable"]}') }}}}{{% endraw %}};
"""

# Ensure directory exists
os.makedirs(model_path, exist_ok=True)

# Write SQL
with open(os.path.join(model_path, f'{target_table}.sql'), 'w') as f:
    f.write(sql)

print(f"✅ dbt model created at {model_path}{target_table}.sql")

