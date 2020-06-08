WITH SOURCE AS
  ( SELECT *
   FROM {{ ref('orders') }} )
SELECT *
FROM SOURCE