WITH SOURCE AS
  ( SELECT *
   FROM {{ ref('input_calendar') }} )
SELECT *
FROM SOURCE