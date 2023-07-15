
  
    

  create  table "financial_data"."financial_clean"."dim_cash_flow__dbt_tmp"
  
  
    as
  
  (
    WITH cash_flow  AS (
    SELECT * FROM "financial_data"."financial_raw"."stg_cash_flow"
),

final AS (
    SELECT *
    FROM cash_flow
)

SELECT * FROM final
  );
  