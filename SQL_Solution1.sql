WITH TEMP AS(
-- order the click based on product_id and date.
SELECT
  product_id,
  click,
  date,
  row_number() over(partition by product_id order by date) as row_num
FROM LidlSheet3
),
--count of clicks based on date i.e. numerator
TEMP2 AS (
SELECT 
  product_id,
  click,
  date,
  row_num,
  MOD(row_num, 30) as mod_val,
  COUNT(case when cast(click as int) = 1 then 1 else 0 end) OVER(PARTITION BY product_id ORDER BY row_num rows BETWEEN current row and 29 following) AS num_of_clicks
FROM TEMP
),
-- count of distinct product_id i.e. denominator
temp3 as (
SELECT
  product_id,
  count(DISTINCT product_id) as distinct_count_of_date
FROM LidlSheet3
group by product_id
)
--final output
SELECT 
	temp2.product_id,
    temp2.date,
    (temp2.num_of_clicks/temp3.distinct_count_of_date)*100 as click_through_rate
FROM temp2 temp2 INNER JOIN temp3 temp3 ON temp2.product_id = temp3.product_id
WHERE temp2.mod_val = 0;

