WITH cash_flow  AS (
    SELECT * FROM "financial_data"."financial_raw"."stg_cash_flow"
),

final AS (
    SELECT *
    FROM cash_flow
)

SELECT * FROM final