WITH price_history  AS (
    SELECT * FROM "financial_data"."financial_raw"."stg_price_history"
),

final AS (
    SELECT *
    FROM price_history
)

SELECT * FROM final