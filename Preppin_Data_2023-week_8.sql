/*No Wolf on Wall Street moments have happened but we've had some big trades happen across a wide range of companies so we need to understand these more.
Can you help us find the top 5 trades for a series of different ways we look at the companies traded and the prices of the trades?

REQUIREMENTS
Create a 'file date' using the month found in the file name
The Null value should be replaced as 1
Clean the Market Cap value to ensure it is the true value as 'Market Capitalisation'
Remove any rows with 'n/a'
Categorise the Purchase Price into groupings
0 to 24,999.99 as 'Low'
25,000 to 49,999.99 as 'Medium'
50,000 to 74,999.99 as 'High'
75,000 to 100,000 as 'Very High'
Categorise the Market Cap into groupings
Below $100M as 'Small'
Between $100M and below $1B as 'Medium'
Between $1B and below $100B as 'Large' 
$100B and above as 'Huge'
Rank the highest 5 purchases per combination of: file date, Purchase Price Categorisation and Market Capitalisation Categorisation.
Output only records with a rank of 1 to 5*/
WITH cte AS
(
SELECT 01 AS FILE_NO ,*
FROM TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK08_01
UNION ALL
SELECT 02 AS FILE_NO ,*
FROM TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK08_02
UNION ALL
SELECT 03 AS FILE_DATE,*
FROM TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK08_03
UNION ALL
SELECT 04 AS FILE_NO,*
FROM TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK08_04
UNION ALL
SELECT 05 AS FILE_NO  ,*
FROM TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK08_05
UNION ALL
SELECT 06 AS FILE_NO ,*
FROM TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK08_06
UNION ALL
SELECT 07 AS FILE_NO ,*
FROM TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK08_07
UNION ALL
SELECT 08 AS FILE_NO,*
FROM TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK08_08
UNION ALL
SELECT 09 AS FILE_NO  ,*
FROM TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK08_09
UNION ALL
SELECT 10 AS FILE_NO ,*
FROM TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK08_10
UNION ALL
SELECT 11 AS FILE_NO ,*
FROM TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK08_11
UNION ALL
SELECT 12 AS FILE_NO ,*
FROM TIL_PLAYGROUND.PREPPIN_DATA_INPUTS.PD2023_WK08_12
)
, cte1 AS
(
SELECT *
      ,DATE_FROM_PARTS(2022,FILE_NO,01) AS file_date
      ,CASE
      WHEN right(market_cap, 1)='B' THEN 1000000000
      WHEN right(market_cap, 1)='M' THEN 1000000
      ELSE 1
      END AS unit
      ,substr(market_cap,2,length(market_cap)-2)::float*unit AS true_market_cap --length-2 makes it dynamic
      ,regexp_substr(purchase_price, '\\d+\\W\\d+')::float AS price --make sure all entities follow same pattern with regexp_like else error
      ,CASE
      WHEN price <25000 THEN 'Low'
      WHEN price <50000 THEN 'Medium'
      WHEN price <75000 THEN 'High'
      ELSE  'Very High'
      END AS price_category
      ,CASE
      WHEN true_market_cap <100000000 THEN 'Small'
      WHEN true_market_cap <1000000000 THEN 'Medium'
      WHEN true_market_cap <100000000000 THEN 'Large'
      ELSE 'Huge' 
      END AS market_cap_category
       --regexp_like(purchase_price,'\\W\\d+\\W\\d+') as match
       --,substr(market_cap,2,length(market_cap)-2)::float as true_market_cap
       from cte 
where market_cap <> 'n/a'
)
,
cte2 AS
(
SELECT *
      ,RANK() OVER(PARTITION BY file_date, market_cap_category, price_category ORDER BY price DESC) AS rnk
      
FROM cte1
)
SELECT 
market_cap_category, 
price_category,
file_date,
ticker,
sector,
market,
stock_name,
market_cap,
purchase_price,
rnk AS rank
FROM cte2 
WHERE rnk <=5;
