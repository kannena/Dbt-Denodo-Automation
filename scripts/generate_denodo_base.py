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
vql_lines = [
    "# REQUIRES-PROPERTIES-FILE - # Do not remove this comment!",
    f'CREATE OR REPLACE WRAPPER JDBC "{target_table}"',
    '    FOLDER = \'/1. virtual model/1. connectivity/2. base views/1. source views\'',
    '    DATASOURCENAME=rsiconnections."ds_CLOUD_DW"',
    f'    CATALOGNAME=\'${{databases.rsicdw.folder.1.. virtual model.folder.1.. connectivity.folder.2.. base views.folder.1.. source views.views.jdbc.{target_table}.CATALOGNAME}}\'',
    f'    SCHEMANAME=\'${{databases.rsicdw.folder.1.. virtual model.folder.1.. connectivity.folder.2.. base views.folder.1.. source views.views.jdbc.{target_table}.SCHEMANAME}}\'',
    f'    RELATIONNAME=\'{target_table}\'',
    '    OUTPUTSCHEMA ('
]

# Add columns to OUTPUTSCHEMA
for col in columns:
    vql_lines.append(
        f'        "{col["name"]}" = \'{col["name"]}\' :\'java.lang.String\' (OPT) '
        f'(sourcetypedecimals=\'0\', sourcetypesize=\'16777216\', description=\'{col["description"]}\', '
        f'sourcetypeid=\'12\', sourcetypename=\'{col["data_type"]}\'),'
    )

# Remove trailing comma from the last column
vql_lines[-1] = vql_lines[-1].rstrip(',')

vql_lines.append('    );')
vql_lines.append('')
vql_lines.append(f'CREATE OR REPLACE TABLE "{target_table}" I18N us_mst (')

# Add columns to CREATE TABLE
for col in columns:
    vql_lines.append(
        f'        "{col["name"]}":{col["data_type"].lower()} '
        f'(sourcetypeid = \'12\', sourcetypedecimals = \'0\', sourcetypesize = \'16777216\', '
        f'description = \'{col["description"]}\'),'
    )

# Remove trailing comma from the last column
vql_lines[-1] = vql_lines[-1].rstrip(',')

vql_lines.append('    );')
vql_lines.append('')
vql_lines.append('    FOLDER = \'/1. virtual model/1. connectivity/2. base views/1. source views\'')
vql_lines.append(f'    DESCRIPTION = \'{data["models"][0]["description"]}\';')

# Ensure path exists
os.makedirs(vql_path, exist_ok=True)

vql_file = os.path.join(vql_path, f'{target_table}_base.vql')
with open(vql_file, 'w') as f:
    f.write("\n".join(vql_lines))

print(f"✅ Enhanced Denodo view VQL created at {vql_file}")
