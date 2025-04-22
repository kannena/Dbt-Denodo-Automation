{{ config(materialized='incremental', tags=['hr', 'employee']) }}

-- Employee master data loaded from HR source system
SELECT
    EMP_ID -- Unique identifier for employee,
    FIRST_NAME -- First name of the employee,
    LAST_NAME -- Last name of the employee,
    HIRE_DATE -- Hire date of the employee
FROM {% raw %}{{ source('HR', 'SRC_EMPLOYEE') }}{% endraw %};