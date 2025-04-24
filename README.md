# DBT & Denodo Automation Framework

This project automates the **dbt models** and **Denodo base and clean views** creation using a configuration-driven approach. It also ensures that **column-level descriptions** are accurately applied to both **Snowflake** and **Denodo**, minimizing manual effort and promoting consistency across environments.

## 🚀 Overview

Traditionally, dbt models and Denodo views are created manually, often leading to repetitive tasks and human errors, especially when managing metadata like column descriptions. This project introduces a GitHub-based automation pipeline that:

- Uses Python scripts to generate configuration templates.
- Accepts human-edited descriptions and model/view metadata.
- Automatically creates and updates dbt SQL models and Denodo VQL views.
- Applies description changes to Snowflake and Denodo environments post-PR merge.

---
## 🔁 Workflow Overview

The following flowchart shows the complete automation process — from local YAML/config generation to GitHub-triggered view creation and description updates:

![image](https://github.com/user-attachments/assets/a0345093-0883-43b7-bacf-23c289721a5f)


---

## 🛠️ Steps to Use

### Step 1: Generate Templates Locally

Run the following Python script to generate two files:

- `<target_table_name>.yaml`: Auto-populated with column names (empty descriptions).
- `template_config.json`: Placeholder config file to be edited.

```bash
python utility/GenerateConfigs.py
```

**Paths:**
- Script: `utility/GenerateConfigs.py`
- Output:
  - YAML file: `<target_table_name>.yaml`
  - Config file: `utility/config.json`

---

### Step 2: Fill in Details

Edit the following:
- **`<target_table_name>.yaml`**: Add meaningful column descriptions (with help from BAs if needed).
- **`config.json`**: Add details like:
  - `SourceTable`
  - `TargetTable`
  - `KeyColumns`
  - `MaterializationType`
  - `SourceApplicationName`
  - `Tags`
  - `DenodoBaseViewPath`
  - `DenodoCleanViewPath`
  - `DenodoBusinessEntityViewPath`
  - `DbtModelPath`

---

### Step 3: Submit PR

- Commit the edited YAML and config files to a new branch.
- Open a Pull Request for review and approval.

---

### Step 4: Automated Generation

Once the PR is merged, a GitHub Action is triggered to:

- Generate **dbt model SQL** with column descriptions.
- Create Denodo **base view** (with descriptions).
- Create Denodo **clean view** (with camelCase columns).
- Place each file into its respective folder as configured.

**Example Output Paths:**
- dbt model: `models/hr/EMPLOYEE.sql`
- Denodo base view: `denodo/views/base/EMPLOYEE_base.vql`
- Denodo clean view: `denodo/views/clean/EMPLOYEE_clean.vql`

---

### Step 5: Updating Descriptions Later

To update descriptions in Snowflake and dbt:

- Modify only the `<target_table_name>.yaml` file with new descriptions.
- Open a new PR.
- On merge, a separate GitHub Action will apply the updated descriptions in:
  - **Snowflake (via metadata update statements)**
  - **dbt models (by updating column comments)**

---

## ⚙️ GitHub Workflows

- `generate_model_and_views.yml`:  
  Triggers after PR merge to generate dbt & Denodo views.

- `update_column_descriptions.yml`:  
  Triggers only if `<target_table_name>.yaml` is modified. Updates descriptions in Snowflake and dbt.

---

## 📂 File Structure Overview

```
utility/
│
├── GenerateConfigs.py            # Script to generate YAML + config templates
├── config.json                   # credentials to connect to snowflake
configs/                   
├── <target_table_name>.yaml      # Filled-in by developer (column descriptions)
├── template_config.json      # Filled-in by developer (source/target info, paths)
│
models/
└── hr/
    └── EMPLOYEE.sql              # Auto-generated dbt model

denodo/
├── views/
│   ├── base/
│   │   └── EMPLOYEE_base.vql     # Auto-generated base view (with descriptions)
│   └── clean/
│       └── EMPLOYEE_clean.vql    # Auto-generated clean view (camelCase columns)
scripts/
├── generate_dbt_model.py
├── generate_denodo_base.py
├── generate_denodo_clean.py
├── update_column_descriptions.py

```

---
