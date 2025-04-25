import sys
import json
import yaml
import os

# Get table name from command-line argument
table_name = sys.argv[1]
json_path = f'configs/{table_name}.json'
yaml_path = f'configs/{table_name}.yaml'

# Load JSON config
with open(json_path) as f:
    config = json.load(f)

# Load YAML for column metadata
with open(yaml_path) as f:
    data = yaml.safe_load(f)

columns = data['models'][0]['columns']
vql_path = config["DenodoBaseViewPath"]

# Mappings
java_type_map = {
    'VARCHAR': 'java.lang.String',
    'NUMBER': 'java.lang.Long',
    'TIMESTAMP_NTZ': 'java.time.LocalDateTime',
    'TIMESTAMPLTZ': 'java.time.OffsetDateTime',
    'DATE': 'java.lang.String'
}

denodo_type_map = {
    'VARCHAR': 'text',
    'NUMBER': 'long',
    'TIMESTAMP_NTZ': 'timestamp',
    'TIMESTAMPLTZ': 'timestamptz',
    'DATE': 'text'
}

# Start writing VQL
vql_lines = [
    '# REQUIRES-PROPERTIES-FILE - # Do not remove this comment!',
    f'CREATE OR REPLACE WRAPPER JDBC "{table_name}"',
    '    FOLDER = \'/1. virtual model/1. connectivity/2. base views/1. source views\'',
    '    DATASOURCENAME=rsiconnections."ds_CLOUD_DW"',
    '    CATALOGNAME=\'DEV_EDW\'',
    '    SCHEMANAME=\'DEVELOP\'',
    f'    RELATIONNAME=\'{table_name}\'',
    '    OUTPUTSCHEMA ('
]

for col in columns:
    java_type = java_type_map.get(col["data_type"], "java.lang.String")
    vql_lines.append(
        f'        "{col["name"]}" = \'{col["name"]}\' :\'{java_type}\' (OPT) '
        f'(sourcetypedecimals=\'0\', sourcetypesize=\'240\', description=\'{col["description"]}\', sourcetypeid=\'12\', sourcetypename=\'{col["data_type"]}\')  SORTABLE,'
    )

vql_lines[-1] = vql_lines[-1].rstrip(',')  # Remove trailing comma
vql_lines.append('    );\n')

# TABLE Definition
vql_lines.append(f'CREATE OR REPLACE TABLE "{table_name}" I18N us_mst (')
for col in columns:
    denodo_type = denodo_type_map.get(col["data_type"], "text")
    vql_lines.append(
        f'        "{col["name"]}":{denodo_type} '
        f'(sourcetypeid=\'12\', sourcetypedecimals=\'0\', sourcetypesize=\'240\', description=\'{col["description"]}\'),'
    )

vql_lines[-1] = vql_lines[-1].rstrip(',')  # Remove trailing comma
vql_lines.append('    )')
vql_lines.append('    FOLDER = \'/1. virtual model/1. connectivity/2. base views/1. source views\'')
vql_lines.append('    CACHE OFF')
vql_lines.append('    TIMETOLIVEINCACHE DEFAULT')

# SEARCHMETHOD block
vql_lines.append(f'    ADD SEARCHMETHOD "{table_name}"(')
vql_lines.append('        I18N us_mst')
vql_lines.append('        CONSTRAINTS (')
for col in columns:
    vql_lines.append(f'             ADD "{col["name"]}" (any) OPT ANY')
vql_lines.append('        )')
output_list = ', '.join(f'"{col["name"]}"' for col in columns)
vql_lines.append(f'        OUTPUTLIST ({output_list})')
vql_lines.append(f'        WRAPPER (jdbc "{table_name}")')
vql_lines.append('    );')

# Save to file
os.makedirs(vql_path, exist_ok=True)
vql_file = os.path.join(vql_path, f"{table_name}_base.vql")
with open(vql_file, 'w') as f:
    f.write("\n".join(vql_lines))

print(f"✅ Denodo base view created at {vql_file}")
