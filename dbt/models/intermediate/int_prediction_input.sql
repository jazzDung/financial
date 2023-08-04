{{ 
  config(
    materialized = 'view',
  ) 
}}

WITH price AS                      
(select  
p.ticker, 
p.trading_date  AS ds, 
close AS y, o.exchange,
LAG(close,1) OVER (
		ORDER BY p.ticker, p.trading_date
	) y_lag

from  AS p 
	INNER JOIN  AS o 
		ON p.ticker = o.ticker
WHERE
  p.trading_date BETWEEN CURRENT_DATE - INTERVAL '366' DAY AND CURRENT_DATE --fix the interval later, this is just for testing
ORDER BY
  ticker,
  ds)
WITH price AS                       
(select
        p.ticker,
        p.trading_date  AS ds,
        close AS y,
        o.exchange,
        LAG(close,
        1) OVER (   
    ORDER BY
        p.ticker,
        p.trading_date  ) y_lag  
    from
        {{ ref('fact_price_history') }} AS p   
    INNER JOIN
        {{ ref('dim_organization') }} AS o    
            ON p.ticker = o.ticker 
    WHERE
        p.trading_date BETWEEN CURRENT_DATE - INTERVAL '366' DAY AND CURRENT_DATE  
    ORDER BY
        ticker,
        ds),    
pre_round as 
(select
        ds,
        y,
        case exchange     
            when 'HOSE' then least(y,
            0.93 * y_lag)     
            when 'HNX' then least(y,
            0.9 * y_lag)     
            when 'UPCOM' then least(y,
            0.85 * y_lag)    
        end floor,
        case exchange     
            when 'HOSE' then greatest(y,
            1.07 * y_lag)     
            when 'HNX' then greatest(y,
            1.1 * y_lag)     
            when 'UPCOM' then greatest(y,
            1.15 * y_lag)    
        end cap   
    from
        price   
    where
        ds BETWEEN CURRENT_DATE - INTERVAL '365' DAY AND CURRENT_DATE)      
select
        ds,
        y,
        round(CAST("floor" as numeric),-2),
        round(CAST("cap" as numeric),-2) 
    from
        pre_round