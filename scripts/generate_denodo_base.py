import sys
import json
import yaml
import os

# Load inputs
table_name = sys.argv[1]
json_path = f'configs/{table_name}.json'
yaml_path = f'configs/{table_name}.yaml'

with open(json_path) as f:
    config = json.load(f)

with open(yaml_path) as f:
    data = yaml.safe_load(f)

columns = data['models'][0]['columns']
vql_path = config["DenodoBaseViewPath"]

# Type mapping
java_type_map = {
    'VARCHAR': 'java.lang.String',
    'NUMBER': 'java.lang.Long',
    'TIMESTAMP_NTZ': 'java.time.LocalDateTime',
    'TIMESTAMPLTZ': 'java.time.OffsetDateTime',
    'DATE': 'java.lang.String'  # or LocalDateTime if required
}

denodo_type_map = {
    'VARCHAR': 'text',
    'NUMBER': 'long',
    'TIMESTAMP_NTZ': 'timestamp',
    'TIMESTAMPLTZ': 'timestamptz',
    'DATE': 'text'
}

# WRAPPER
vql_lines = [
    '# REQUIRES-PROPERTIES-FILE - # Do not remove this comment!',
    f'CREATE OR REPLACE WRAPPER JDBC "{table_name}"',
    '    FOLDER = \'/1. virtual model/1. connectivity/2. base views/1. source views\'',
    '    DATASOURCENAME=rsiconnections."ds_CLOUD_DW"',
    '    CATALOGNAME=\'DEV_EDW\'',
    '    SCHEMANAME=\'DEVELOP\'',
    f'    RELATIONNAME=\'{table_name}\'',
    '    OUTPUTSCHEMA ('
