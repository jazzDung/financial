WITH income_statement  AS (
    SELECT * FROM {{ref ('stg_income_statement')}}
),

final AS (
    SELECT *
    FROM income_statement
)

SELECT * FROM final