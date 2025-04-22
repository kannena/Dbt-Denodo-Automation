import json
import yaml
import os
import re

def to_camel_case(s):
    return ''.join(word.capitalize() for word in re.split(r'_| ', s.lower()))

# Load config
with open('configs/template_config.json') as f:
    config = json.load(f)

target_table = config["TargetTable"]
vql_path = config["DenodoCleanViewPath"]
yaml_path = f'configs/{target_table}.yaml'

with open(yaml_path) as f:
    data = yaml.safe_load(f)

columns = data['models'][0]['columns']

# Build SELECT lines
select_lines = []
for col in columns:
    camel_case = to_camel_case(col["name"])
    select_lines.append(f'  {col["name"]} AS {camel_case} (description = "{col["description"]}")')

select_block = ",\n".join(select_lines)

# Final VQL
vql = (
    f"CREATE OR REPLACE VIEW {target_table}_clean AS\n"
    f"SELECT\n"
    f"{select_block}\n"
    f"FROM {target_table};"
)

# Ensure path exists
os.makedirs(vql_path, exist_ok=True)

vql_file = os.path.join(vql_path, f'{target_table}_clean.vql')
with open(vql_file, 'w') as f:
    f.write(vql)

print(f"✅ Clean Denodo view VQL created at {vql_file}")
