
import json
import yaml
import os

# Load config
with open('configs/template_config.json') as f:
    config = json.load(f)

target_table = config["TargetTable"]
vql_path = config["DenodoBaseViewPath"]
yaml_path = f'configs/{target_table}.yaml'

with open(yaml_path) as f:
    data = yaml.safe_load(f)

columns = data['models'][0]['columns']

# VQL base
vql_lines = [f"CREATE OR REPLACE BASE VIEW {target_table} ("]

for col in columns:
    vql_lines.append(f'  {col["name"]} {col["data_type"]} (description = "{col["description"]}"),')

vql_lines[-1] = vql_lines[-1].rstrip(',')  # remove trailing comma
vql_lines.append(");")

# Ensure path exists
os.makedirs(vql_path, exist_ok=True)

vql_file = os.path.join(vql_path, f'{target_table}_base.vql')
with open(vql_file, 'w') as f:
    f.write("\n".join(vql_lines))

print(f"✅ Base Denodo view VQL created at {vql_file}")
