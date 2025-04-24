
{
    config(
        materialized='incremental',
        unique_key='PK_EMPLOYEE_ID',
        merge_no_update_columns=['SYS_CREATE_DTM'],
        tags=['hr', 'employee']
    )
}

-- Employee master data loaded from HR source system
WITH
GET_NEW_RECORDS AS (
  SELECT *, 1 AS BATCH_KEY_ID
  FROM
  {% raw %}{{ source('HR', 'SRC_EMPLOYEE') }}{% endraw %}
  {% if is_incremental() %}
  WHERE
  SF_INSERT_TIMESTAMP > '{{ get_max_event_time('SF_INSERT_TIMESTAMP', not_minus3=True) }}'
  {% endif %}
),
DEDUPE_CTE AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY EMP_ID ORDER BY SF_INSERT_TIMESTAMP DESC) AS ROW_NUM
  FROM GET_NEW_RECORDS
),
INS_BATCH_ID AS (
    SELECT TO_NUMBER(TO_VARCHAR(CURRENT_TIMESTAMP, 'YYYYMMDDHH24MISSFF3')) AS INS_BATCH_ID, 1 AS BATCH_KEY_ID
)

SELECT
  { generate_surrogate_key(['EMP_ID']) } AS PK_EMPLOYEE_ID,
  CURRENT_TIMESTAMP AS SYS_CREATE_DTM,
  CURRENT_TIMESTAMP AS SYS_LAST_UPDATE_DTM,
  INS_BATCH_ID AS SYS_EXEC_ID,
  { string_to_number('EMP_ID', 38, 0) } AS EMP_ID -- Unique identifier for employee 12,
  { set_varchar_length('FIRST_NAME', 240) } AS FIRST_NAME -- First name of the employee,
  { set_varchar_length('LAST_NAME', 240) } AS LAST_NAME -- Last name of the employee,
  { string_to_timezone_ntz('HIRE_DATE') } AS HIRE_DATE -- Hire date of the employee
FROM DEDUPE_CTE
LEFT JOIN INS_BATCH_ID USING (BATCH_KEY_ID)
WHERE ROW_NUM = 1;
