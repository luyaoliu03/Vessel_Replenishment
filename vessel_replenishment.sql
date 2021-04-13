-- Create Inventory table to replicate Agustin's Inventory Evolution table

-- Create inventory table who only has update records
DROP TABLE IF EXISTS inventory;
CREATE TEMP TABLE inventory AS

SELECT a.updated_at AS inventory_update_utc_timestamp
     , DATEADD('HOUR', SUBSTRING(a.updated_at,20,3)::integer, LEFT(a.updated_at,19)::timestamp) AS inventory_update_local_timestamp
     , c.name AS inventory_location
     , a.location_id
     , d.id AS variant_id
     , d.sku AS sku_id
     , e.product_type
     , e.title AS product_title
     , a.available AS inventory_quantity
     , a.current_indicator AS current_inventory_indicator
	 , a.schema_src
FROM stockup_mart_vw.inventory_levels a
LEFT JOIN stockup_mart_vw.products__variants d ON a.inventory_item_id = d.inventory_item_id
LEFT JOIN stockup_mart_vw.products e ON d._sdc_source_key_id = e.id
LEFT JOIN stockup_mart_vw.locations c ON a.location_id = c.id
WHERE NOT (inventory_location = 'MIA-003' AND variant_id = 34047167496332 AND inventory_update_utc_timestamp = '2020-11-15T10:20:45-05:00') --remove incorrect inventory input
AND inventory_location <> 'MIA-027 Warehouse Receiving'
AND inventory_location <> 'GVA-Training'
AND inventory_location <> 'MIA-027-KW01'
AND inventory_location <> 'NYC -Training'
AND inventory_location <> 'NYC-Test'
AND inventory_location <> 'LAX - Test'
;


-- Convert only-update into start-end-date inventory level
DROP TABLE IF EXISTS start_end_date_inventory;
CREATE TEMP TABLE start_end_date_inventory AS

SELECT inventory_location
     , location_id
     , inventory_update_local_timestamp
     , variant_id
     , sku_id
     , product_type
     , product_title
     , inventory_quantity
     , start_timestamp
     , CASE WHEN end_timestamp ::timestamp IS NULL THEN DATEADD('HOUR', SUBSTRING(inventory_update_utc_timestamp,20,3)::integer, sysdate) ELSE end_timestamp ::timestamp END AS end_timestamp
     , start_timestamp ::date AS start_date
     , CASE WHEN end_timestamp ::timestamp IS NULL THEN DATEADD('HOUR', SUBSTRING(inventory_update_utc_timestamp,20,3)::integer, sysdate) ::date ELSE DATEADD('DAY',-1, end_timestamp ::date) ::date END AS end_date
     , current_inventory_indicator
	 , schema_src
FROM (
     SELECT inventory_location
            , location_id
            , inventory_update_utc_timestamp
            , inventory_update_local_timestamp
            , variant_id
            , sku_id
            , product_type
            , product_title
            , inventory_quantity
            , inventory_update_local_timestamp AS start_timestamp
            , LAG(inventory_update_local_timestamp) OVER (PARTITION BY inventory_location, variant_id ORDER BY inventory_update_local_timestamp DESC) AS end_timestamp
            , current_inventory_indicator
            , schema_src
      FROM inventory
    )
;

-- Only keep the latest record per day for each variant in each location
DROP TABLE IF EXISTS start_end_date_inventory_dedup;
CREATE TEMP TABLE start_end_date_inventory_dedup AS

SELECT DISTINCT *
FROM start_end_date_inventory
WHERE (inventory_location, variant_id, start_timestamp) IN (
    SELECT inventory_location
         , variant_id
         , MAX(start_timestamp) AS start_timestamp
    FROM start_end_date_inventory
    GROUP BY 1,2,start_date
    )
;


-- Create a date table includes today
DROP TABLE IF EXISTS reference_dates;

CREATE TEMP TABLE reference_dates AS

SELECT DATEADD(DAY, 2, date) ::date AS date
     , DATE_PART(dow, date) ::integer AS day_of_week_num
     , CASE
         WHEN day_of_week_num = 0 THEN 'Tuesday'
         WHEN day_of_week_num = 1 THEN 'Wednesday'
         WHEN day_of_week_num = 2 THEN 'Thursday'
         WHEN day_of_week_num = 3 THEN 'Friday'
         WHEN day_of_week_num = 4 THEN 'Saturday'
         WHEN day_of_week_num = 5 THEN 'Sunday'
         WHEN day_of_week_num = 6 THEN 'Monday'
         END ::text AS day_of_week
FROM reference.dates
WHERE date >= '2020-09-07'
;

-- Convert start-end-date level into daily inventory level
DROP TABLE IF EXISTS daily_inventory;

CREATE TEMP TABLE daily_inventory AS

SELECT b.date
     , b.day_of_week
     , a.inventory_location
     , SUBSTRING(inventory_location,1,3)::text AS msa
     , a.location_id
     , a.variant_id
     , a.sku_id
     , a.product_type
     , a.product_title
     , a.inventory_quantity
     , a.current_inventory_indicator
	 , a.schema_src
FROM start_end_date_inventory_dedup a
JOIN reference_dates b ON b.date BETWEEN a.start_date AND a.end_date
;

--select distinct name from stockup_mart_vw.locations order by 1;

-- Create Product table, pick out the latest refreshed product table of each day
DROP TABLE IF EXISTS product;

CREATE TEMP TABLE product AS

SELECT CONVERT_TIMEZONE('UTC', 'US/Eastern', load_date) AS load_timestamp
     , load_date ::date AS date
     , variant_id_shopify
     , product_name
     , CASE WHEN macro_category = '0' THEN NULL ELSE macro_category END AS macro_category
     , CASE WHEN micro_category = '0' THEN NULL ELSE micro_category END AS micro_category
     , target_selection
     , qty_per_bin
     , qty_of_bins
	 , CASE
	     WHEN msa = 'Miami' THEN 'MIA'
	     WHEN msa = 'New York' THEN 'NYC'
		 WHEN msa = 'Los Angeles' THEN 'LAX'
		 WHEN msa = 'San Francisco' THEN 'SFO'
		 ELSE msa END AS msa
FROM grocery.product
WHERE variant_id_shopify <> 1
AND variant_id_shopify IS NOT NULL
AND (load_date) IN (
    SELECT MAX(load_date)
    FROM grocery.product
    GROUP BY load_date ::date
    )
AND msa IS NOT NULL
;


------------------  For Vessel Replenishment Dashboard
DROP TABLE IF EXISTS vessel_sales;

CREATE TEMP TABLE vessel_sales AS

WITH last7 AS (
    SELECT CONVERT_TIMEZONE('UTC', 'US/Eastern', created_at ::timestamp)::date AS local_order_date
         , DATEADD('DAY', -7, local_order_date) ::date AS last_7_day_begin_date
         , sales_location_id
         , COUNT(*) AS count_of_orders
    FROM (
             SELECT created_at
                  , (customer__first_name || ' ' || customer__last_name) ::text AS customer_name
                  , CASE
                        WHEN customer_name IN ('DP 014', 'DP Sales 014') THEN 45648871564
                        WHEN customer_name = 'DP Sales 017' THEN 45674758284
                        WHEN customer_name = 'DP Sales 01' THEN 43395416204
                        ELSE location_id
                 END ::bigint AS sales_location_id
                  , id AS order_id
             FROM stockup_mart_vw.orders
             WHERE sales_location_id IS NOT NULL
         )
    GROUP BY 1,2,3
    --ORDER BY 1,2,3 ASC
)

SELECT l1.local_order_date AS local_order_date
     , l1.sales_location_id AS sales_location_id
     , SUM(l2.count_of_orders) AS last_7_day_sales
FROM last7 l1
LEFT JOIN last7 l2
ON l2.local_order_date BETWEEN l1.last_7_day_begin_date AND l1.local_order_date
AND l1.sales_location_id = l2.sales_location_id
GROUP BY 1,2
--ORDER BY 1,2
;

-- To generate vessel replenishment table

DROP TABLE IF EXISTS vessel_replenishment;

CREATE TEMP TABLE vessel_replenishment AS

SELECT a.date
     , b.load_timestamp AS product_updated_at
     , CONVERT_TIMEZONE('UTC', 'US/Eastern', sysdate) AS updated_timestamp
     , a.inventory_location
     , a.variant_id
     , a.sku_id
     , b.product_name
     , b.macro_category
     , b.micro_category
     , a.inventory_quantity
     , a.current_inventory_indicator
     , LOWER(b.target_selection) AS target_selection
     , b.qty_per_bin
     , b.qty_of_bins
     --, CASE WHEN b.qty_per_bin <> 0 THEN TRUNC( (a.inventory_quantity / b.qty_per_bin), 1 ) END AS qty_of_bins_current
     --, FLOOR(b.qty_of_bins - qty_of_bins_current) AS bin_replenishment
     --, (bin_replenishment * b.qty_per_bin) AS qty_replenishment
     , (b.qty_of_bins * b.qty_per_bin - a.inventory_quantity) AS qty_replenishment
     , CASE WHEN last_7_day_sales IS NULL THEN 0 ELSE last_7_day_sales END AS last_7_day_sales
	 , a.msa AS msa
FROM daily_inventory a
JOIN product b ON a.date = b.date AND a.variant_id = b.variant_id_shopify AND a.msa = b.msa
LEFT JOIN vessel_sales c ON a.date = c.local_order_date AND a.location_id = c.sales_location_id
;

-- Create the table into grocery schema and filter out the inventory = 0 in MIA-027 Warehouse Sellable records

--DELETE FROM grocery.vessel_replenishment;
--INSERT INTO grocery.vessel_replenishment

DROP TABLE IF EXISTS grocery.vessel_replenishment;

CREATE TABLE grocery.vessel_replenishment AS

SELECT *
FROM vessel_replenishment
WHERE (date, variant_id, msa) NOT IN (
    SELECT date, variant_id, msa
    FROM vessel_replenishment
    WHERE ( inventory_location = 'MIA-027 Warehouse Sellable'
    OR inventory_location = 'NYC-Warehouse'
    OR inventory_location = 'LAX-Warehouse' )
    AND inventory_quantity = 0
    )
;


------------------  For Retail Score Card

-- Create Vessel Sales 2 table
DROP TABLE IF EXISTS sales;

CREATE TEMP TABLE sales AS

SELECT DATEADD('HOUR', -4, a.created_at ::timestamp) ::date AS local_order_date
    , (a.customer__first_name || ' ' || a.customer__last_name) ::text AS customer_name
    , CASE
        WHEN customer_name IN ('DP 014', 'DP Sales 014') THEN 45648871564
        WHEN customer_name = 'DP Sales 017' THEN 45674758284
        WHEN customer_name = 'DP Sales 01' THEN 43395416204
        ELSE a.location_id END ::bigint AS sales_location_id
    , CASE WHEN a.cancelled_at IS NOT NULL THEN TRUE ELSE FALSE END AS is_cancelled
    , b.variant_id
    , CASE WHEN (b.quantity ::numeric) IS NULL THEN 0 ELSE b.quantity END AS sales_quantity
    , CASE WHEN (d.quantity ::numeric) IS NULL THEN 0 ELSE d.quantity END AS refund_quantity
FROM stockup_mart_vw.orders a
LEFT JOIN stockup_mart_vw.orders__line_items b ON a.id = b._sdc_source_key_id
LEFT JOIN stockup_mart_vw.orders__refunds__refund_line_items d ON b.id = d.line_item_id
WHERE local_order_date >= '2020-09-09'
AND is_cancelled = False
AND b.variant_id IS NOT NULL
;

DROP TABLE IF EXISTS sales_dedup;

CREATE TEMP TABLE sales_dedup AS

SELECT a.sales_location_id
     , a.local_order_date
     , a.variant_id
     , SUM(a.sales_quantity) AS sales_quantity
     , SUM(a.refund_quantity) AS refund_quantity
FROM sales a
GROUP BY 1,2,3
;

DROP TABLE IF EXISTS vessel_sales_2;

CREATE TEMP TABLE vessel_sales_2 AS

SELECT local_order_date
     , sales_location_id
     , variant_id
     , (sales_quantity - refund_quantity) AS sku_sales_quantity
FROM sales_dedup
WHERE sales_location_id IS NOT NULL
;


DROP TABLE IF EXISTS vessel_sales_2_null;

CREATE TEMP TABLE vessel_sales_2_null AS

SELECT local_order_date
     , variant_id
     , (sales_quantity - refund_quantity) AS sku_sales_quantity
FROM sales_dedup
WHERE sales_location_id IS NULL
;


-- Create a date table includes today
DROP TABLE IF EXISTS reference_dates_2;

CREATE TEMP TABLE reference_dates_2 AS

SELECT DISTINCT a.date
              , b.variant_id
FROM reference_dates a
LEFT JOIN (
    SELECT local_order_date
         , variant_id
    FROM vessel_sales_2_null
    ) b
ON b.local_order_date BETWEEN (SELECT MIN(date) FROM reference_dates) AND (SELECT MAX(date) FROM reference_dates)
--order by 2,1 asc
;


-- Join the table --
-- 01: To create last 7 days begin date column
DROP TABLE IF EXISTS score_card_1;

CREATE TEMP TABLE score_card_1 AS

WITH daily_inventory_sales AS (
    SELECT a.date
         , DATEADD('DAY', -7, a.date) ::date AS last_7_days_begin
         , a.day_of_week
         , a.inventory_location AS location_code
         , a.location_id
         , a.msa
         , a.variant_id
         , a.inventory_quantity
         , a.current_inventory_indicator
         , CASE WHEN b.sku_sales_quantity IS NULL THEN 0 ELSE b.sku_sales_quantity END AS sku_sales_quantity
    FROM daily_inventory a
    LEFT JOIN vessel_sales_2 b ON a.date = b.local_order_date AND a.location_id = b.sales_location_id AND a.variant_id = b.variant_id
    UNION
    SELECT c.date AS date
         , DATEADD('DAY', -7, date) ::date AS last_7_days_begin
         , CASE
             WHEN DATE_PART(dow, date) = 0 THEN 'Sunday'
             WHEN DATE_PART(dow, date) = 1 THEN 'Monday'
             WHEN DATE_PART(dow, date) = 2 THEN 'Tuesday'
             WHEN DATE_PART(dow, date) = 3 THEN 'Wednesday'
             WHEN DATE_PART(dow, date) = 4 THEN 'Thursday'
             WHEN DATE_PART(dow, date) = 5 THEN 'Friday'
             WHEN DATE_PART(dow, date) = 6 THEN 'Saturday'
             END ::text AS day_of_week
         , 'MIA-blanks' ::text AS location_code
         , 00000000000 ::bigint AS location_id
         , 'Miami'AS msa
         , c.variant_id
         , NULL AS inventory_quantity
         , NULL AS current_inventory_indicator
         , CASE WHEN d.sku_sales_quantity IS NULL THEN 0 ELSE d.sku_sales_quantity END AS sku_sales_quantity
    FROM reference_dates_2 c
    LEFT JOIN vessel_sales_2_null d ON c.date = d.local_order_date AND c.variant_id = d.variant_id
    --order by 7,4,1 asc
)

SELECT d.*
     , p.macro_category
     , p.micro_category
     , LOWER(p.target_selection) AS target_selection
     , p.load_timestamp
FROM daily_inventory_sales d
LEFT JOIN product p ON d.date = p.date AND d.variant_id = p.variant_id_shopify
;


-- 02: To calculate last 7 days sales on variant-location level
DELETE FROM grocery.score_card;

INSERT INTO grocery.score_card

WITH agg_sku AS (
SELECT s1.date
     , s1.location_id
     , s1.variant_id
     , CASE
         WHEN s1.date < '2020-09-16' THEN NULL ELSE sum(s2.sku_sales_quantity) END AS last_7_days_sku_sales --bc sales info starts from 09/09
FROM score_card_1 s1
LEFT JOIN score_card_1 s2
ON s2.date BETWEEN s1.last_7_days_begin AND DATEADD('DAY', -1, s1.date)
AND s1.location_id = s2.location_id AND s1.variant_id = s2.variant_id
GROUP BY 1,2,3
                )

SELECT a.*
     , b.last_7_days_sku_sales
FROM score_card_1 a
LEFT JOIN agg_sku b
ON a.date = b.date AND a.location_id = b.location_id AND a.variant_id = b.variant_id
;
