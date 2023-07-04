WITH balance_sheet  AS (
    SELECT * FROM {{ref ('stg_balance_sheet')}}
),

final AS (
    SELECT *
    FROM balance_sheet
)

SELECT * FROM final