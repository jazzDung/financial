select distinct on (ticker) 
	ticker as ticker,
	businessModel as business_model,
	businessEfficiency as business_efficiency,
	assetQuality as asset_quality,
	cashFlowQuality as cash_flow_quality,
	bom as bom,
	businessAdministration as business_administration,
	productService as product_service,
	businessAdvantage as business_advantage,
	companyPosition as company_position,
	industry as industry,
	operationRisk as operation_risk,
	_airbyte_emitted_at::date as index_date
from
	{{ source('source', 'business_model_rating') }}
where 
    ticker is not null