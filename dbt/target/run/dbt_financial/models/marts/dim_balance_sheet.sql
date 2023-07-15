
  
    

  create  table "financial_data"."financial_clean"."dim_balance_sheet__dbt_tmp"
  
  
    as
  
  (
    WITH balance_sheet  AS (
    SELECT * FROM "financial_data"."financial_raw"."stg_balance_sheet"
),

final AS (
    SELECT *
    FROM balance_sheet
)

SELECT * FROM final
  );
  