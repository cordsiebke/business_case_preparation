WITH SOURCE AS
  ( SELECT *
   FROM {{ ref('input_orders') }} )
SELECT *
FROM SOURCE