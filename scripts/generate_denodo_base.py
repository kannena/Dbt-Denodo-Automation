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
vql_path = config["DenodoBaseViewPath"]

vql_lines = [
    "# REQUIRES-PROPERTIES-FILE - # Do not remove this comment!",
    f'CREATE OR REPLACE WRAPPER JDBC "{table_name}"',
    '    FOLDER = '/1. virtual model/1. connectivity/2. base views/1. source views'',
    '    DATASOURCENAME=rsiconnections."ds_CLOUD_DW"',
    f'    RELATIONNAME='{table_name}'',
    '    OUTPUTSCHEMA ('
]

for col in columns:
    vql_lines.append(
        f'        "{col["name"]}" = '{col["name"]}' :'java.lang.String' (OPT) '
        f'(sourcetypedecimals='0', sourcetypesize='16777216', description='{col["description"]}', '
        f'sourcetypeid='12', sourcetypename='{col["data_type"]}'),'
    )
vql_lines[-1] = vql_lines[-1].rstrip(',')
vql_lines.append('    );')
vql_lines.append('')
vql_lines.append(f'CREATE OR REPLACE TABLE "{table_name}" I18N us_mst (')

for col in columns:
    vql_lines.append(
        f'        "{col["name"]}":{col["data_type"].lower()} '
        f'(sourcetypeid = '12', sourcetypedecimals = '0', sourcetypesize = '16777216', '
        f'description = '{col["description"]}'),'
    )
vql_lines[-1] = vql_lines[-1].rstrip(',')
vql_lines.append('    );')
vql_lines.append(f'    DESCRIPTION = '{data["models"][0]["description"]}';')

os.makedirs(vql_path, exist_ok=True)
vql_file = os.path.join(vql_path, f'{table_name}_base.vql')
with open(vql_file, 'w') as f:
    f.write("\n".join(vql_lines))

print(f"✅ Denodo base view created at {vql_file}")
