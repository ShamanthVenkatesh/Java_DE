WITH TEMP AS(
-- order the click based on product_id and date.
SELECT
  product_id,
  click,
  date,
  ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY date) AS row_num
FROM LidlSheet3
),
--count of clicks based on date i.e. numerator
TEMP2 AS (
SELECT 
  product_id,
  click,
  date,
  row_num,
  MOD(row_num, 30) AS mod_val,
--RAN IT ON https://sqliteonline.com/ using postgres, due to which click = true / false being treated as 1 / 0.
  COUNT(CASE WHEN CAST(click AS int) = 1 THEN 1 ELSE 0 END) OVER(PARTITION BY product_id ORDER BY row_num ROWS BETWEEN CURRENT ROW AND 29 FOLLOWING) AS num_of_clicks
FROM TEMP
),
-- count of distinct product_id i.e. denominator
temp3 AS (
SELECT
  product_id,
  COUNT(DISTINCT product_id) AS distinct_count_of_date
FROM LidlSheet3
GROUP BY product_id
),
temp4 AS (
SELECT 
	temp2.product_id,
    temp2.date,
    (temp2.num_of_clicks/temp3.distinct_count_of_date)*100 AS click_through_rate
FROM temp2 temp2 INNER JOIN temp3 temp3 ON temp2.product_id = temp3.product_id
WHERE temp2.mod_val = 0
),
--join with products table.
joined AS (
SELECT 
	temp4.product_id,
	temp4.click_through_rate,
	category.price
FROM temp4 temp4 INNER JOIN products category ON temp4.product_id = category.product_id
),
--formation of price tier.
interim AS (
SELECT 
	product_id,
	click_through_rate,
	CASE
		WHEN (price >= 0 AND price < 5) THEN "0-5"
		WHEN (price >= 5 AND price <10) THEN "5-10"
		WHEN (price >= 10 AND price <15) THEN "10-15"
		WHEN (price >= 15 ) THEN ">15"
		ELSE "UNKOWN_RANGE"
	END AS price
FROM joined
)
--final output
SELECT price, COUNT(click_through_rate) AS cnts
FROM interim
GROUP BY price;

