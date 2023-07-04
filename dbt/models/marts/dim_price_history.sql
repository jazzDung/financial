WITH price_history  AS (
    SELECT * FROM {{ref ('stg_price_history')}}
),

final AS (
    SELECT *
    FROM price_history
)

SELECT * FROM final