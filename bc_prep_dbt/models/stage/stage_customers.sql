WITH SOURCE AS
  ( SELECT *
   FROM {{ ref('input_customers') }} 
   WHERE (customer_migration_flag = 'f' AND customer_merge_flag = 'f')
   )
SELECT *
FROM SOURCE