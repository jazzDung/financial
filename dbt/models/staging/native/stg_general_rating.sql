
SELECT stockRating AS stock_rating,
       valuation,
       financialHealth AS financial_health,
       businessModel AS business_model,
       businessOperation AS business_operation,
       rsRating AS rs_rating,
       taScore AS ta_score,
       ticker,
       highestPrice AS highest_price,
       lowestPrice AS lowest_price,
       priceChange3m AS price_change_3m,
       priceChange1y AS price_change_1y,
       beta,
       alpha
FROM {{source('general_rating', 'general_rating')}}