
  
    

  create  table "financial_data"."financial_clean"."dim_general_rating__dbt_tmp"
  
  
    as
  
  (
    WITH general_rating  AS (
    SELECT * FROM "financial_data"."financial_raw"."stg_general_rating"
),

final AS (
    SELECT *
    FROM general_rating
)

SELECT * FROM final
  );
  