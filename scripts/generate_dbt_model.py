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
model_path = config["Dbtmodelpath"]
source_table = config["SourceTable"]
source_app = config["SourceApplicationName"]
materialization_type = config["Metir# filepath: c:\Users\kannena\Downloads\GitAutomation\generate_dbt_model.py
import sys
import json
import yaml
import os
import re

table_name = sys.argv[1]
json_path = f'configs/{table_name}.json'
yaml_path = f'configs/{table_name}.yaml'

with open(json_path) as f:
    config = json.load)

with open(yaml_path) as f:
    data = yaml.safe_load(f)

columns = data['models'][0]['columns']
model_path = config["Dbtmodelpath"]
source_table = config["SourceTable"]
source_app = config["SourceApplicationName"]
materialization_type = config["Metir
