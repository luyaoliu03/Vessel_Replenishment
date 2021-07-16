-- Created scratch table to delete wrong historical data from Shopify (scratch.wrong_historical_data)

-- Create inventory table who only has update records
DROP TABLE IF EXISTS inventory;
CREATE TEMP TABLE inventory AS

-- Only keep the latest record per day for each variant in each location
WITH inventory_levels AS (
    SELECT *
    FROM (
	    SELECT updated_at
	         , inventory_item_id
	         , location_id
	         , available AS inventory_quantity
	         , current_indicator AS current_inventory_indicator
	         , schema_src
	         , ROW_NUMBER() OVER (PARTITION BY inventory_item_id, location_id, updated_at ::date ORDER BY updated_at DESC) AS rn
	    FROM stockup_mart_vw.inventory_levels
        )
	WHERE rn = 1

), products__variants AS (
	SELECT DISTINCT _sdc_source_key_id AS product_id
	              , id AS variant_id
	              , inventory_item_id
	              , sku AS sku_id
	FROM stockup_mart_vw.products__variants

), products AS (
	SELECT DISTINCT id AS product_id
	              , product_type
	              , title AS product_title
	FROM stockup_mart_vw.products

), locations AS (
	SELECT DISTINCT id AS location_id
	              , name AS inventory_location
	FROM stockup_mart_vw.locations
    WHERE name <> 'MIA-027 Warehouse Receiving'
	AND name <> 'GVA-Training'
	AND name <> 'MIA-027-KW01'
	AND name <> 'NYC -Training'
	AND name <> 'NYC-Test'
	AND name <> 'LAX - Test'
	AND name <> '601 Brickell Key Dr'
	AND name <> 'Office Snacks'
	AND name <> 'test 1'
	AND name <> 'test 2'
)

SELECT a.updated_at AS inventory_update_utc_timestamp
     , DATEADD('HOUR', SUBSTRING(a.updated_at,20,3)::integer, LEFT(a.updated_at,19)::timestamp) AS inventory_update_local_timestamp
     , d.inventory_location
     , a.location_id
     , b.variant_id
     , b.sku_id
     , c.product_type
     , c.product_title
     , a.inventory_quantity
     , a.current_inventory_indicator
	 , a.schema_src
FROM inventory_levels a
LEFT JOIN products__variants b ON a.inventory_item_id = b.inventory_item_id
LEFT JOIN products c ON b.product_id = c.product_id
LEFT JOIN locations d ON a.location_id = d.location_id
;


--delete the wrong historical data
DELETE FROM inventory WHERE (inventory_location, variant_id, inventory_update_utc_timestamp) IN
 (select inventory_location, variant_id, inventory_update_utc_timestamp from scratch.wrong_historical_data);

--backfill the correct inventory record into inventory table
INSERT INTO inventory (inventory_update_utc_timestamp, inventory_update_local_timestamp, inventory_location, location_id, variant_id, sku_id, product_type, product_title,inventory_quantity,current_inventory_indicator,schema_src)
VALUES ('2021-06-08T00:38:18-04:00', '2021-06-07 20:38:18.000000', 'NYC-014-GV01', '61416046791', '39410381914311', '10-1-244', NULL, 'Gushers Flavor Mixer (4.25 oz)', 16, 'N', 'stockup_mart_ny');


-- Convert only-update into start-end-date inventory level
DROP TABLE IF EXISTS start_end_date_inventory;
CREATE TEMP TABLE start_end_date_inventory AS

WITH pre AS (
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
FROM pre
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


-- Create Product table, pick out the latest refreshed product table of each day
DROP TABLE IF EXISTS product;

CREATE TEMP TABLE product AS

WITH pre AS (
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
		     WHEN msa = 'Dallas' THEN 'DAL'
		     WHEN msa = 'Portland' THEN 'POR'
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
)

SELECT load_timestamp
	 , date
	 , variant_id_shopify
	 , product_name
	 , macro_category
	 , micro_category
	 , target_selection
	 , qty_per_bin
	 , qty_of_bins
	 , CASE WHEN msa = 'BAL/WAS' THEN 'BAL' ELSE msa END AS msa
FROM pre

UNION

SELECT load_timestamp
	 , date
	 , variant_id_shopify
	 , product_name
	 , macro_category
	 , micro_category
	 , target_selection
	 , qty_per_bin
	 , qty_of_bins
	 , CASE WHEN msa = 'BAL/WAS' THEN 'WAS' END AS msa
FROM pre
WHERE msa = 'BAL/WAS'
;


-- Convert start-end-date level into daily inventory level
DROP TABLE IF EXISTS daily_inventory;

CREATE TEMP TABLE daily_inventory AS

SELECT b.date
     , b.day_of_week
     , a.inventory_location
     , (
         case when inventory_location = 'Robomart Pharmacy & Snacks' then 'LAX'
         else SUBSTRING(inventory_location,1,3)::text
         end
    ) AS msa
     , a.location_id
     , a.variant_id
     , a.sku_id
     , a.product_type
     , a.product_title
     , a.inventory_quantity
     , a.current_inventory_indicator
	 , a.schema_src
FROM start_end_date_inventory a
JOIN reference_dates b ON b.date BETWEEN a.start_date AND a.end_date
;


------------------  For Vessel Replenishment Dashboard
--DROP TABLE IF EXISTS vessel_sales;

--CREATE TEMP TABLE vessel_sales AS

--WITH last7 AS (
--    SELECT CONVERT_TIMEZONE('UTC', 'US/Eastern', created_at ::timestamp)::date AS local_order_date
--         , DATEADD('DAY', -7, local_order_date) ::date AS last_7_day_begin_date
--         , sales_location_id
--         , COUNT(*) AS count_of_orders
--    FROM (
--             SELECT created_at
--                  , (customer__first_name || ' ' || customer__last_name) ::text AS customer_name
--                  , CASE
--                        WHEN customer_name IN ('DP 014', 'DP Sales 014') THEN 45648871564
--                        WHEN customer_name = 'DP Sales 017' THEN 45674758284
--                        WHEN customer_name = 'DP Sales 01' THEN 43395416204
--                        ELSE location_id
--                 END ::bigint AS sales_location_id
--                  , id AS order_id
--             FROM stockup_mart_vw.orders
--             WHERE sales_location_id IS NOT NULL
--         )
--    GROUP BY 1,2,3
    --ORDER BY 1,2,3 ASC
--)

--SELECT l1.local_order_date AS local_order_date
--     , l1.sales_location_id AS sales_location_id
--     , SUM(l2.count_of_orders) AS last_7_day_sales
--FROM last7 l1
--LEFT JOIN last7 l2
--ON l2.local_order_date BETWEEN l1.last_7_day_begin_date AND l1.local_order_date
--AND l1.sales_location_id = l2.sales_location_id
--GROUP BY 1,2
--ORDER BY 1,2
--;

-- To generate vessel replenishment table

DROP TABLE IF EXISTS grocery.daily_location_inventory;

CREATE  TABLE grocery.daily_location_inventory AS

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
     --, CASE WHEN last_7_day_sales IS NULL THEN 0 ELSE last_7_day_sales END AS last_7_day_sales
	 , a.msa AS msa
FROM daily_inventory a
JOIN product b ON a.date = b.date AND a.variant_id = b.variant_id_shopify AND a.msa = b.msa
--LEFT JOIN vessel_sales c ON a.date = c.local_order_date AND a.location_id = c.sales_location_id
;


-- Create the table into grocery schema and filter out the inventory = 0 in MIA-027 Warehouse Sellable records

--DELETE FROM grocery.vessel_replenishment;
--INSERT INTO grocery.vessel_replenishment

DROP TABLE IF EXISTS grocery.vessel_replenishment;

CREATE  TABLE grocery.vessel_replenishment AS

SELECT *
FROM grocery.daily_location_inventory
WHERE (date, variant_id, msa) IN (
    SELECT date, variant_id, msa
    FROM grocery.daily_location_inventory
    WHERE ( inventory_location = 'MIA-027 Warehouse Sellable'
    OR inventory_location = 'NYC-Warehouse'
    OR inventory_location = 'LAX-Warehouse'
    OR inventory_location = 'DAL - Warehouse'
    OR inventory_location = 'SFO - Warehouse'
    OR inventory_location = 'POR-Warehouse'
    OR inventory_location = 'NSH-Warehouse'
    OR inventory_location = 'AUS-Warehouse'
    OR inventory_location = 'ATL-Warehouse'
    OR inventory_location = 'HOU-033-GT01'
    OR inventory_location = 'MIN-027-GW01'
    OR inventory_location = 'BAL-031-GW01'
    OR inventory_location = 'DET-012-GW01'
    OR inventory_location = 'HOU-033-GT01'
    OR inventory_location = 'PHI-040-GW01'
    OR inventory_location = 'SAT-015-GW01')
    AND inventory_quantity > 0
    )
;

------------------  For Retail Score Card

-- Create Vessel Sales 2 table
DROP TABLE IF EXISTS sales;

CREATE TEMP TABLE sales AS

WITH orders AS (
    SELECT DISTINCT DATEADD('HOUR', -4, created_at ::timestamp) ::date AS local_order_date
         , id
         , cancelled_at
         , schema_src
    FROM stockup_mart_vw.orders
    WHERE local_order_date >= '2020-09-09'

), orders__line_items AS (
    SELECT DISTINCT _sdc_source_key_id
         , variant_id
         , id
         , quantity
    FROM stockup_mart_vw.orders__line_items
    WHERE variant_id IS NOT NULL

), orders__refunds__refund_line_items AS (
    SELECT DISTINCT line_item_id
         , quantity
    FROM stockup_mart_vw.orders__refunds__refund_line_items

)

SELECT local_order_date
    --, (a.customer__first_name || ' ' || a.customer__last_name) ::text AS customer_name
    --, CASE
    --   WHEN customer_name IN ('DP 014', 'DP Sales 014') THEN 45648871564
    --    WHEN customer_name = 'DP Sales 017' THEN 45674758284
    --    WHEN customer_name = 'DP Sales 01' THEN 43395416204
    --    ELSE a.location_id END ::bigint AS sales_location_id
    , a.schema_src
    , CASE WHEN a.cancelled_at IS NOT NULL THEN TRUE ELSE FALSE END AS is_cancelled
    , b.variant_id
    , CASE WHEN (b.quantity ::numeric) IS NULL THEN 0 ELSE b.quantity END AS sales_quantity
    , CASE WHEN (c.quantity ::numeric) IS NULL THEN 0 ELSE c.quantity END AS refund_quantity
FROM orders a
LEFT JOIN orders__line_items b ON a.id = b._sdc_source_key_id
LEFT JOIN orders__refunds__refund_line_items c ON b.id = c.line_item_id
WHERE is_cancelled = False
;


DROP TABLE IF EXISTS vessel_sales_2;

CREATE TEMP TABLE vessel_sales_2 AS

WITH dedup AS (
	SELECT a.schema_src
	     , a.local_order_date
	     , a.variant_id
	     , SUM(a.sales_quantity) AS sales_quantity
	     , SUM(a.refund_quantity) AS refund_quantity
	FROM sales a
	GROUP BY 1,2,3
)

SELECT local_order_date
     , schema_src
     , variant_id
     , (sales_quantity - refund_quantity) AS sku_sales_quantity
FROM dedup
;

DROP TABLE IF EXISTS daily_msa_inventory;

CREATE TEMP TABLE daily_msa_inventory AS

SELECT date
     , day_of_week
     , schema_src
     , variant_id
     , SUM(inventory_quantity) AS inventory_quantity
FROM daily_inventory
GROUP BY 1,2,3,4
;


--DROP TABLE IF EXISTS vessel_sales_2_null;

--CREATE TEMP TABLE vessel_sales_2_null AS

--SELECT local_order_date
--     , variant_id
--     , (sales_quantity - refund_quantity) AS sku_sales_quantity
--FROM sales_dedup
--WHERE sales_location_id IS NULL
--;


-- Create a date table includes today
--DROP TABLE IF EXISTS reference_dates_2;

--CREATE TEMP TABLE reference_dates_2 AS

--SELECT DISTINCT a.date
--              , b.variant_id
--FROM reference_dates a
--LEFT JOIN (
--    SELECT local_order_date
--         , variant_id
--    FROM vessel_sales_2_null
--    ) b
--ON b.local_order_date BETWEEN (SELECT MIN(date) FROM reference_dates) AND (SELECT MAX(date) FROM reference_dates)
--order by 2,1 asc
--;


-- Join the table --
-- 01: To create last 7 days begin date column
DROP TABLE IF EXISTS score_card_1;

CREATE TEMP TABLE score_card_1 AS

WITH daily_inventory_sales AS (
    SELECT a.date
         , DATEADD('DAY', -7, a.date) ::date AS last_7_days_begin
         , a.day_of_week
         , a.schema_src
         , CASE
	         WHEN a.schema_src = 'stockup_mart_sf' THEN 'SFO'
	         WHEN a.schema_src = 'stockup_mart_la' THEN 'LAX'
	         WHEN a.schema_src = 'stockup_mart_ny' THEN 'NYC'
	         WHEN a.schema_src = 'stockup_mart_pdx' THEN 'POR'
	         WHEN a.schema_src = 'stockup_mart' THEN 'MIA'
	         WHEN a.schema_src = 'stockup_mart_nsh' THEN 'NSH'
	         WHEN a.schema_src = 'stockup_mart_atx' THEN 'AUS'
	         WHEN a.schema_src = 'stockup_mart_dl' THEN 'DAL'
             WHEN a.schema_src = 'stockup_mart_atl' THEN 'ATL'
             WHEN a.schema_src = 'stockup_mart_hou' THEN 'HOU'
             WHEN a.schema_src = 'stockup_mart_mnp' THEN 'MIN'
             WHEN a.schema_src = 'stockup_mart_bal' THEN 'BAL'
             WHEN a.schema_src = 'stockup_mart_chi' THEN 'CHI'
             WHEN a.schema_src = 'stockup_mart_dc' THEN 'WAS'
             WHEN a.schema_src = 'stockup_mart_det' THEN 'DEL'
             WHEN a.schema_src = 'stockup_mart_phl' THEN 'PHI'
             WHEN a.schema_src = 'stockup_mart_sea' THEN 'SEA'
             WHEN a.schema_src = 'stockup_mart_san' THEN 'SAN'
	        END AS msa
         , a.variant_id
         , a.inventory_quantity
         , CASE WHEN b.sku_sales_quantity IS NULL THEN 0 ELSE b.sku_sales_quantity END AS sku_sales_quantity
    FROM daily_msa_inventory a
    LEFT JOIN vessel_sales_2 b ON a.date = b.local_order_date AND a.schema_src = b.schema_src AND a.variant_id = b.variant_id

--    UNION

--    SELECT c.date AS date
--         , DATEADD('DAY', -7, date) ::date AS last_7_days_begin
--         , CASE
--             WHEN DATE_PART(dow, date) = 0 THEN 'Sunday'
--             WHEN DATE_PART(dow, date) = 1 THEN 'Monday'
--             WHEN DATE_PART(dow, date) = 2 THEN 'Tuesday'
--             WHEN DATE_PART(dow, date) = 3 THEN 'Wednesday'
--             WHEN DATE_PART(dow, date) = 4 THEN 'Thursday'
--             WHEN DATE_PART(dow, date) = 5 THEN 'Friday'
--             WHEN DATE_PART(dow, date) = 6 THEN 'Saturday'
--             END ::text AS day_of_week
--         , 'MIA-blanks' ::text AS location_code
--         , 00000000000 ::bigint AS location_id
--         , 'Miami'AS msa
--         , c.variant_id
--        , NULL AS inventory_quantity
--         , NULL AS current_inventory_indicator
--         , CASE WHEN d.sku_sales_quantity IS NULL THEN 0 ELSE d.sku_sales_quantity END AS sku_sales_quantity
--    FROM reference_dates_2 c
--    LEFT JOIN vessel_sales_2_null d ON c.date = d.local_order_date AND c.variant_id = d.variant_id
    --order by 7,4,1 asc
)

SELECT d.date
     , d.last_7_days_begin
     , d.day_of_week
	 , d.msa
     , d.variant_id
     , d.inventory_quantity
     , d.sku_sales_quantity
     , p.macro_category
     , p.micro_category
     , LOWER(p.target_selection) AS target_selection
     , p.load_timestamp
FROM daily_inventory_sales d
LEFT JOIN product p ON d.date = p.date AND d.variant_id = p.variant_id_shopify AND d.msa = p.msa
;


-- 02: To calculate last 7 days sales on variant-location level
--DELETE FROM grocery.score_card;

--INSERT INTO grocery.score_card

DROP TABLE IF EXISTS grocery.score_card;;

CREATE TABLE grocery.score_card AS

WITH agg_sku AS (
SELECT s1.date
     , s1.msa
     , s1.variant_id
     , CASE
         WHEN s1.date < '2020-09-16' THEN NULL ELSE sum(s2.sku_sales_quantity) END AS last_7_days_sku_sales --bc sales info starts from 09/09
FROM score_card_1 s1
LEFT JOIN score_card_1 s2
ON s2.date BETWEEN s1.last_7_days_begin AND DATEADD('DAY', -1, s1.date)
AND s1.msa = s2.msa AND s1.variant_id = s2.variant_id
GROUP BY 1,2,3
                )

SELECT a.*
     , b.last_7_days_sku_sales
FROM score_card_1 a
LEFT JOIN agg_sku b
ON a.date = b.date AND a.msa = b.msa AND a.variant_id = b.variant_id
;



