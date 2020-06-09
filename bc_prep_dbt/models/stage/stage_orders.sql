WITH SOURCE AS
  ( SELECT *
   FROM {{ ref('input_orders') }} 
   WHERE order_migration_flag = 'f'
   )
SELECT *
FROM SOURCE