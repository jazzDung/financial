WITH cash_flow  AS (
    SELECT * FROM {{ref ('stg_cash_flow')}}
),

final AS (
    SELECT *
    FROM cash_flow
)

SELECT * FROM final