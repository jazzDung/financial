WITH general_rating  AS (
    SELECT * FROM {{ref ('stg_general_rating')}}
),

final AS (
    SELECT *
    FROM general_rating
)

SELECT * FROM final