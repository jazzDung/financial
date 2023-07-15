WITH general_rating  AS (
    SELECT * FROM "financial_data"."financial_raw"."stg_general_rating"
),

final AS (
    SELECT *
    FROM general_rating
)

SELECT * FROM final