WITH SOURCE AS
  ( SELECT *
   FROM {{ ref('input_customers') }} )
SELECT *
FROM SOURCE