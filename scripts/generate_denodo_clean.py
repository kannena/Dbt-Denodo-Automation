import sys
import json
import yaml
import os
import re

table_name = sys.argv[1]
json_path = f'configs/{table_name}.json'
yaml_path = f'configs/{table_name}.yaml'

with open(json_path) as f:
    config = json.load(f)

with open(yaml_path) as f:
    data = yaml.safe_load(f)

columns = data['models'][0]['columns']
vql_path = config["DenodoCleanViewPath"]

def to_camel_case(s):
    return ''.join(word.capitalize() for word in re.split(r'_| ', s.lower()))

select_lines = []
for col in columns:
    camel_case = to_camel_case(col["name"])
    select_lines.append(f'  {col["name"]} AS {camel_case} (description = "{col["description"]}")')

select_block = ",\n".join(select_lines)

vql = (
    f"CREATE OR REPLACE VIEW {table_name}_clean AS\n"
    f"SELECT\n"
    f"{select_block}\n"
    f"FROM {table_name};"
)

os.makedirs(vql_path, exist_ok=True)
vql_file = os.path.join(vql_path, f'{table_name}_clean.vql')
with open(vql_file, 'w') as f:
    f.write(vql)

print(f"✅ Denodo clean view created at {vql_file}")
