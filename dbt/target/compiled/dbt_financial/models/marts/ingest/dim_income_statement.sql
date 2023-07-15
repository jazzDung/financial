WITH income_statement  AS (
    SELECT * FROM "financial_data"."financial_raw"."stg_income_statement"
),

final AS (
    SELECT *
    FROM income_statement
)

SELECT * FROM final