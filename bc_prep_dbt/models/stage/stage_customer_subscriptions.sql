WITH SOURCE AS
  ( SELECT *
   FROM {{ ref('input_subscriptions') }} )
SELECT *
FROM SOURCE