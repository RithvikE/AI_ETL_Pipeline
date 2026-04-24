CREATE TABLE IF NOT EXISTS AI_ETL.AI_ETL_WI.TRF_ORDER_INVENTORY_FACT AS
WITH orders_raw AS (
    SELECT
        o.SALES_ORDER_NO,
        o.PRODUCTID,
        o.SUPPLIERID,
        o.CUSTOMERID,
        COALESCE(
            TRY_TO_DATE(TO_VARCHAR(o."DATE"), 'DD-MM-YYYY'),
            TRY_TO_DATE(TO_VARCHAR(o."DATE"), 'YYYY-MM-DD'),
            TRY_TO_DATE(o."DATE")
        ) AS ORDER_DATE,
        COALESCE(
            TRY_TO_DATE(TO_VARCHAR(o.EXPECTED_DELIVERY_DATE), 'DD-MM-YYYY'),
            TRY_TO_DATE(TO_VARCHAR(o.EXPECTED_DELIVERY_DATE), 'YYYY-MM-DD'),
            TRY_TO_DATE(o.EXPECTED_DELIVERY_DATE)
        ) AS EXPECTED_DELIVERY_DATE,
        COALESCE(
            TRY_TO_DATE(TO_VARCHAR(o.ACTUAL_DELIVERY_DATE), 'DD-MM-YYYY'),
            TRY_TO_DATE(TO_VARCHAR(o.ACTUAL_DELIVERY_DATE), 'YYYY-MM-DD'),
            TRY_TO_DATE(o.ACTUAL_DELIVERY_DATE)
        ) AS ACTUAL_DELIVERY_DATE,
        o.SALES_QUANTITY,
        o.ACCURATE_ORDER,
        o."RETURN" AS RETURN_FLAG,
        o.DEFECTIVE_UNIT,
        o.DEFECT_COUNT,
        o.MODE_OF_ORDER,
        o.MODE_OF_SHIPPING,
        o.SHIPPING_COST,
        o.ON_TIME_SHIPPING_FLAG,
        o.ON_TIME_DELIVERY,
        o.SELLING_PRICE,
        o.SALES_QUANTITY__BY_SELLING_PRICE,
        o.DISCOUNT_PER,
        o.FINAL_PRICE,
        o.CUSTOMER_SEGMENT
    FROM AI_ETL.AI_ETL_STG.STG_ORDERS o
),
orders_dedup AS (
    SELECT
        orders_raw.*,
        ROW_NUMBER() OVER (
            PARTITION BY orders_raw.SALES_ORDER_NO, orders_raw.PRODUCTID, orders_raw.SUPPLIERID
            ORDER BY orders_raw.ORDER_DATE DESC
        ) AS rn
    FROM orders_raw
),
orders_latest AS (
    SELECT
        SALES_ORDER_NO,
        PRODUCTID,
        SUPPLIERID,
        CUSTOMERID,
        ORDER_DATE,
        EXPECTED_DELIVERY_DATE,
        ACTUAL_DELIVERY_DATE,
        SALES_QUANTITY,
        ACCURATE_ORDER,
        RETURN_FLAG,
        DEFECTIVE_UNIT,
        DEFECT_COUNT,
        MODE_OF_ORDER,
        MODE_OF_SHIPPING,
        SHIPPING_COST,
        ON_TIME_SHIPPING_FLAG,
        ON_TIME_DELIVERY,
        SELLING_PRICE,
        SALES_QUANTITY__BY_SELLING_PRICE,
        DISCOUNT_PER,
        FINAL_PRICE,
        CUSTOMER_SEGMENT
    FROM orders_dedup
    WHERE rn = 1
),
inventory_raw AS (
    SELECT
        inv.PRODUCT_ID,
        inv.SUPPLIER_ID,
        inv.INVENTORY_UNITS,
        inv.IN_OUT_STOCK,
        inv.NO_OF_DAYS_OUT_OF_STOCK,
        inv.INVENTORY_SERVICE_COSTS,
        inv.INVENTORY_RISK_COSTS,
        inv.STORAGE_COST,
        inv.INVENTORY_CARRYING_COST,
        COALESCE(
            TRY_TO_DATE(TO_VARCHAR(inv.INVENTORY_UPDATE_DATE), 'DD-MM-YYYY'),
            TRY_TO_DATE(TO_VARCHAR(inv.INVENTORY_UPDATE_DATE), 'YYYY-MM-DD'),
            TRY_TO_DATE(inv.INVENTORY_UPDATE_DATE)
        ) AS INVENTORY_UPDATE_DATE
    FROM AI_ETL.AI_ETL_STG.STG_INVENTORY inv
),
inventory_dedup AS (
    SELECT
        inventory_raw.*,
        ROW_NUMBER() OVER (
            PARTITION BY inventory_raw.PRODUCT_ID, inventory_raw.SUPPLIER_ID
            ORDER BY inventory_raw.INVENTORY_UPDATE_DATE DESC
        ) AS rn
    FROM inventory_raw
),
inventory_latest AS (
    SELECT
        PRODUCT_ID,
        SUPPLIER_ID,
        INVENTORY_UNITS,
        IN_OUT_STOCK,
        NO_OF_DAYS_OUT_OF_STOCK,
        INVENTORY_SERVICE_COSTS,
        INVENTORY_RISK_COSTS,
        STORAGE_COST,
        INVENTORY_CARRYING_COST,
        INVENTORY_UPDATE_DATE
    FROM inventory_dedup
    WHERE rn = 1
),
product_raw AS (
    SELECT
        p.PRODUCT_ID,
        p.PRODUCT_NAME,
        p.PRODUCT_COMPONENT,
        p.UNIT_PRICE,
        COALESCE(
            TRY_TO_DATE(TO_VARCHAR(p.EOL_DATE), 'DD-MM-YYYY'),
            TRY_TO_DATE(TO_VARCHAR(p.EOL_DATE), 'YYYY-MM-DD'),
            TRY_TO_DATE(p.EOL_DATE)
        ) AS PRODUCT_EOL_DATE
    FROM AI_ETL.AI_ETL_STG.STG_PRODUCT p
),
product_dedup AS (
    SELECT
        product_raw.*,
        ROW_NUMBER() OVER (
            PARTITION BY product_raw.PRODUCT_ID
            ORDER BY product_raw.PRODUCT_EOL_DATE DESC
        ) AS rn
    FROM product_raw
),
product_latest AS (
    SELECT
        PRODUCT_ID,
        PRODUCT_NAME,
        PRODUCT_COMPONENT,
        UNIT_PRICE,
        PRODUCT_EOL_DATE
    FROM product_dedup
    WHERE rn = 1
),
supplier_raw AS (
    SELECT
        s.SUPPLIER_ID,
        s.SUPPLIER_NAME,
        s.SUPPLIER_CITY,
        s.STATE AS SUPPLIER_STATE,
        s.COUNTRY AS SUPPLIER_COUNTRY,
        s.ROAD_TRANSPORT_COST,
        s.SEA_TRANSPORT_COST,
        s.AIR_TRANSPORT_COST
    FROM AI_ETL.AI_ETL_STG.STG_SUPPLIER s
),
supplier_dedup AS (
    SELECT
        supplier_raw.*,
        ROW_NUMBER() OVER (
            PARTITION BY supplier_raw.SUPPLIER_ID
            ORDER BY supplier_raw.SUPPLIER_NAME ASC
        ) AS rn
    FROM supplier_raw
),
supplier_latest AS (
    SELECT
        SUPPLIER_ID,
        SUPPLIER_NAME,
        SUPPLIER_CITY,
        SUPPLIER_STATE,
        SUPPLIER_COUNTRY,
        ROAD_TRANSPORT_COST,
        SEA_TRANSPORT_COST,
        AIR_TRANSPORT_COST
    FROM supplier_dedup
    WHERE rn = 1
),
customer_raw AS (
    SELECT
        c.CUSTOMERID,
        c.CUSTOMER_NAME,
        c.CUSTOMER_CITY,
        c.CUSTOMER_STATE,
        c.CUSTOMER_COUNTRY
    FROM AI_ETL.AI_ETL_STG.STG_CUSTOMER c
),
customer_dedup AS (
    SELECT
        customer_raw.*,
        ROW_NUMBER() OVER (
            PARTITION BY customer_raw.CUSTOMERID
            ORDER BY customer_raw.CUSTOMER_NAME ASC
        ) AS rn
    FROM customer_raw
),
customer_latest AS (
    SELECT
        CUSTOMERID,
        CUSTOMER_NAME,
        CUSTOMER_CITY,
        CUSTOMER_STATE,
        CUSTOMER_COUNTRY
    FROM customer_dedup
    WHERE rn = 1
),
assembled AS (
    SELECT
        ol.SALES_ORDER_NO,
        ol.PRODUCTID,
        ol.SUPPLIERID,
        ol.CUSTOMERID,
        cust.CUSTOMER_NAME,
        cust.CUSTOMER_CITY,
        cust.CUSTOMER_STATE,
        cust.CUSTOMER_COUNTRY,
        ol.ORDER_DATE,
        ol.EXPECTED_DELIVERY_DATE,
        ol.ACTUAL_DELIVERY_DATE,
        ol.SALES_QUANTITY,
        ol.ACCURATE_ORDER,
        ol.RETURN_FLAG,
        ol.DEFECTIVE_UNIT,
        ol.DEFECT_COUNT,
        ol.MODE_OF_ORDER,
        ol.MODE_OF_SHIPPING,
        ol.SHIPPING_COST,
        ol.ON_TIME_SHIPPING_FLAG,
        ol.ON_TIME_DELIVERY,
        ol.SELLING_PRICE,
        ol.SALES_QUANTITY__BY_SELLING_PRICE,
        ol.DISCOUNT_PER,
        ol.FINAL_PRICE,
        ol.CUSTOMER_SEGMENT,
        pr.PRODUCT_NAME,
        pr.PRODUCT_COMPONENT,
        pr.UNIT_PRICE,
        pr.PRODUCT_EOL_DATE,
        sup.SUPPLIER_NAME,
        sup.SUPPLIER_CITY,
        sup.SUPPLIER_STATE,
        sup.SUPPLIER_COUNTRY,
        inv.INVENTORY_UNITS,
        inv.IN_OUT_STOCK,
        inv.NO_OF_DAYS_OUT_OF_STOCK,
        inv.INVENTORY_SERVICE_COSTS,
        inv.INVENTORY_RISK_COSTS,
        inv.STORAGE_COST,
        inv.INVENTORY_CARRYING_COST,
        inv.INVENTORY_UPDATE_DATE,
        CASE
            WHEN UPPER(ol.MODE_OF_SHIPPING) = 'AIR' THEN COALESCE(sup.AIR_TRANSPORT_COST, 0)
            WHEN UPPER(ol.MODE_OF_SHIPPING) = 'SEA' THEN COALESCE(sup.SEA_TRANSPORT_COST, 0)
            WHEN UPPER(ol.MODE_OF_SHIPPING) = 'ROAD' THEN COALESCE(sup.ROAD_TRANSPORT_COST, 0)
            ELSE 0
        END AS SHIPPING_FACTOR
    FROM orders_latest ol
    LEFT JOIN inventory_latest inv
        ON ol.PRODUCTID = inv.PRODUCT_ID
       AND ol.SUPPLIERID = inv.SUPPLIER_ID
    LEFT JOIN product_latest pr
        ON ol.PRODUCTID = pr.PRODUCT_ID
    LEFT JOIN supplier_latest sup
        ON ol.SUPPLIERID = sup.SUPPLIER_ID
    LEFT JOIN customer_latest cust
        ON ol.CUSTOMERID = cust.CUSTOMERID
),
final_ranked AS (
    SELECT
        assembled.SALES_ORDER_NO,
        assembled.PRODUCTID,
        assembled.SUPPLIERID,
        assembled.CUSTOMERID,
        assembled.CUSTOMER_NAME,
        assembled.CUSTOMER_CITY,
        assembled.CUSTOMER_STATE,
        assembled.CUSTOMER_COUNTRY,
        assembled.ORDER_DATE,
        assembled.EXPECTED_DELIVERY_DATE,
        assembled.ACTUAL_DELIVERY_DATE,
        assembled.SALES_QUANTITY,
        assembled.ACCURATE_ORDER,
        assembled.RETURN_FLAG,
        assembled.DEFECTIVE_UNIT,
        assembled.DEFECT_COUNT,
        assembled.MODE_OF_ORDER,
        assembled.MODE_OF_SHIPPING,
        assembled.SHIPPING_COST,
        assembled.ON_TIME_SHIPPING_FLAG,
        assembled.ON_TIME_DELIVERY,
        assembled.SELLING_PRICE,
        assembled.SALES_QUANTITY__BY_SELLING_PRICE,
        assembled.DISCOUNT_PER,
        assembled.FINAL_PRICE,
        assembled.CUSTOMER_SEGMENT,
        assembled.PRODUCT_NAME,
        assembled.PRODUCT_COMPONENT,
        assembled.UNIT_PRICE,
        assembled.PRODUCT_EOL_DATE,
        assembled.SUPPLIER_NAME,
        assembled.SUPPLIER_CITY,
        assembled.SUPPLIER_STATE,
        assembled.SUPPLIER_COUNTRY,
        assembled.INVENTORY_UNITS,
        assembled.IN_OUT_STOCK,
        assembled.NO_OF_DAYS_OUT_OF_STOCK,
        assembled.INVENTORY_SERVICE_COSTS,
        assembled.INVENTORY_RISK_COSTS,
        assembled.STORAGE_COST,
        assembled.INVENTORY_CARRYING_COST,
        assembled.INVENTORY_UPDATE_DATE,
        assembled.SHIPPING_FACTOR,
        CASE WHEN assembled.ON_TIME_DELIVERY IS TRUE AND assembled.ACCURATE_ORDER IS TRUE THEN 1 ELSE 0 END AS OTIF_FLAG,
        COALESCE(assembled.UNIT_PRICE, 0) + COALESCE(assembled.SHIPPING_COST, 0) + COALESCE(assembled.INVENTORY_SERVICE_COSTS, 0) + COALESCE(assembled.STORAGE_COST, 0) AS LANDED_COST_PER_UNIT,
        CASE WHEN assembled.SALES_QUANTITY IS NOT NULL AND assembled.SALES_QUANTITY <> 0 THEN COALESCE(assembled.INVENTORY_UNITS, 0) / assembled.SALES_QUANTITY ELSE NULL END AS DSI_ESTIMATE,
        CASE
            WHEN COALESCE(assembled.UNIT_PRICE, 0) * COALESCE(assembled.INVENTORY_UNITS, 0) <> 0
            THEN ((COALESCE(assembled.FINAL_PRICE, 0) - COALESCE(assembled.UNIT_PRICE, 0)) * COALESCE(assembled.SALES_QUANTITY, 0)) / (COALESCE(assembled.UNIT_PRICE, 0) * COALESCE(assembled.INVENTORY_UNITS, 0))
            ELSE NULL
        END AS GMROI,
        CASE
            WHEN assembled.ORDER_DATE IS NOT NULL AND assembled.ACTUAL_DELIVERY_DATE IS NOT NULL
            THEN DATEDIFF('DAY', assembled.ORDER_DATE, assembled.ACTUAL_DELIVERY_DATE)
            ELSE NULL
        END AS ORDER_TO_CASH_CYCLE_DAYS,
        CASE
            WHEN assembled.SALES_QUANTITY IS NOT NULL AND assembled.SALES_QUANTITY <> 0
            THEN (CASE WHEN assembled.RETURN_FLAG IS TRUE THEN 1 ELSE 0 END) / assembled.SALES_QUANTITY
            ELSE NULL
        END AS RETURN_TO_SALES_RATIO,
        CASE
            WHEN assembled.SALES_QUANTITY IS NOT NULL AND assembled.SALES_QUANTITY <> 0
            THEN COALESCE(assembled.SHIPPING_COST, 0) / assembled.SALES_QUANTITY
            ELSE NULL
        END AS LAST_MILE_EFFICIENCY,
        CASE
            WHEN assembled.SALES_QUANTITY IS NOT NULL AND assembled.SALES_QUANTITY <> 0
            THEN COALESCE(assembled.NO_OF_DAYS_OUT_OF_STOCK, 0) / assembled.SALES_QUANTITY
            ELSE NULL
        END AS STOCKOUT_RISK_SCORE,
        CASE
            WHEN assembled.SALES_QUANTITY IS NOT NULL
            THEN assembled.SALES_QUANTITY * COALESCE(assembled.SHIPPING_FACTOR, 0)
            ELSE NULL
        END AS CARBON_FOOTPRINT_EST,
        CASE
            WHEN assembled.EXPECTED_DELIVERY_DATE IS NOT NULL AND assembled.ACTUAL_DELIVERY_DATE IS NOT NULL
            THEN DATEDIFF('DAY', assembled.EXPECTED_DELIVERY_DATE, assembled.ACTUAL_DELIVERY_DATE)
            ELSE NULL
        END AS CARRIER_LEAD_TIME_VARIANCE,
        ROW_NUMBER() OVER (
            PARTITION BY assembled.SALES_ORDER_NO, assembled.PRODUCTID, assembled.SUPPLIERID
            ORDER BY assembled.ORDER_DATE DESC, assembled.INVENTORY_UPDATE_DATE DESC, assembled.FINAL_PRICE DESC
        ) AS rn
    FROM assembled
)
SELECT
    final_ranked.SALES_ORDER_NO,
    final_ranked.PRODUCTID,
    final_ranked.SUPPLIERID,
    final_ranked.CUSTOMERID,
    final_ranked.CUSTOMER_NAME,
    final_ranked.CUSTOMER_CITY,
    final_ranked.CUSTOMER_STATE,
    final_ranked.CUSTOMER_COUNTRY,
    final_ranked.ORDER_DATE,
    final_ranked.EXPECTED_DELIVERY_DATE,
    final_ranked.ACTUAL_DELIVERY_DATE,
    final_ranked.SALES_QUANTITY,
    final_ranked.ACCURATE_ORDER,
    final_ranked.RETURN_FLAG,
    final_ranked.DEFECTIVE_UNIT,
    final_ranked.DEFECT_COUNT,
    final_ranked.MODE_OF_ORDER,
    final_ranked.MODE_OF_SHIPPING,
    final_ranked.SHIPPING_COST,
    final_ranked.ON_TIME_SHIPPING_FLAG,
    final_ranked.ON_TIME_DELIVERY,
    final_ranked.SELLING_PRICE,
    final_ranked.SALES_QUANTITY__BY_SELLING_PRICE,
    final_ranked.DISCOUNT_PER,
    final_ranked.FINAL_PRICE,
    final_ranked.CUSTOMER_SEGMENT,
    final_ranked.PRODUCT_NAME,
    final_ranked.PRODUCT_COMPONENT,
    final_ranked.UNIT_PRICE,
    final_ranked.PRODUCT_EOL_DATE,
    final_ranked.SUPPLIER_NAME,
    final_ranked.SUPPLIER_CITY,
    final_ranked.SUPPLIER_STATE,
    final_ranked.SUPPLIER_COUNTRY,
    final_ranked.INVENTORY_UNITS,
    final_ranked.IN_OUT_STOCK,
    final_ranked.NO_OF_DAYS_OUT_OF_STOCK,
    final_ranked.INVENTORY_SERVICE_COSTS,
    final_ranked.INVENTORY_RISK_COSTS,
    final_ranked.STORAGE_COST,
    final_ranked.INVENTORY_CARRYING_COST,
    final_ranked.INVENTORY_UPDATE_DATE,
    final_ranked.SHIPPING_FACTOR,
    final_ranked.OTIF_FLAG,
    final_ranked.LANDED_COST_PER_UNIT,
    final_ranked.DSI_ESTIMATE,
    final_ranked.GMROI,
    final_ranked.ORDER_TO_CASH_CYCLE_DAYS,
    final_ranked.RETURN_TO_SALES_RATIO,
    final_ranked.LAST_MILE_EFFICIENCY,
    final_ranked.STOCKOUT_RISK_SCORE,
    final_ranked.CARBON_FOOTPRINT_EST,
    final_ranked.CARRIER_LEAD_TIME_VARIANCE
FROM final_ranked
WHERE final_ranked.rn = 1;

MERGE INTO AI_ETL.AI_ETL_WI.TRF_ORDER_INVENTORY_FACT T
USING (
WITH orders_raw AS (
    SELECT
        o.SALES_ORDER_NO,
        o.PRODUCTID,
        o.SUPPLIERID,
        o.CUSTOMERID,
        COALESCE(
            TRY_TO_DATE(TO_VARCHAR(o."DATE"), 'DD-MM-YYYY'),
            TRY_TO_DATE(TO_VARCHAR(o."DATE"), 'YYYY-MM-DD'),
            TRY_TO_DATE(o."DATE")
        ) AS ORDER_DATE,
        COALESCE(
            TRY_TO_DATE(TO_VARCHAR(o.EXPECTED_DELIVERY_DATE), 'DD-MM-YYYY'),
            TRY_TO_DATE(TO_VARCHAR(o.EXPECTED_DELIVERY_DATE), 'YYYY-MM-DD'),
            TRY_TO_DATE(o.EXPECTED_DELIVERY_DATE)
        ) AS EXPECTED_DELIVERY_DATE,
        COALESCE(
            TRY_TO_DATE(TO_VARCHAR(o.ACTUAL_DELIVERY_DATE), 'DD-MM-YYYY'),
            TRY_TO_DATE(TO_VARCHAR(o.ACTUAL_DELIVERY_DATE), 'YYYY-MM-DD'),
            TRY_TO_DATE(o.ACTUAL_DELIVERY_DATE)
        ) AS ACTUAL_DELIVERY_DATE,
        o.SALES_QUANTITY,
        o.ACCURATE_ORDER,
        o."RETURN" AS RETURN_FLAG,
        o.DEFECTIVE_UNIT,
        o.DEFECT_COUNT,
        o.MODE_OF_ORDER,
        o.MODE_OF_SHIPPING,
        o.SHIPPING_COST,
        o.ON_TIME_SHIPPING_FLAG,
        o.ON_TIME_DELIVERY,
        o.SELLING_PRICE,
        o.SALES_QUANTITY__BY_SELLING_PRICE,
        o.DISCOUNT_PER,
        o.FINAL_PRICE,
        o.CUSTOMER_SEGMENT
    FROM AI_ETL.AI_ETL_STG.STG_ORDERS o
),
orders_dedup AS (
    SELECT
        orders_raw.*,
        ROW_NUMBER() OVER (
            PARTITION BY orders_raw.SALES_ORDER_NO, orders_raw.PRODUCTID, orders_raw.SUPPLIERID
            ORDER BY orders_raw.ORDER_DATE DESC
        ) AS rn
    FROM orders_raw
),
orders_latest AS (
    SELECT
        SALES_ORDER_NO,
        PRODUCTID,
        SUPPLIERID,
        CUSTOMERID,
        ORDER_DATE,
        EXPECTED_DELIVERY_DATE,
        ACTUAL_DELIVERY_DATE,
        SALES_QUANTITY,
        ACCURATE_ORDER,
        RETURN_FLAG,
        DEFECTIVE_UNIT,
        DEFECT_COUNT,
        MODE_OF_ORDER,
        MODE_OF_SHIPPING,
        SHIPPING_COST,
        ON_TIME_SHIPPING_FLAG,
        ON_TIME_DELIVERY,
        SELLING_PRICE,
        SALES_QUANTITY__BY_SELLING_PRICE,
        DISCOUNT_PER,
        FINAL_PRICE,
        CUSTOMER_SEGMENT
    FROM orders_dedup
    WHERE rn = 1
),
inventory_raw AS (
    SELECT
        inv.PRODUCT_ID,
        inv.SUPPLIER_ID,
        inv.INVENTORY_UNITS,
        inv.IN_OUT_STOCK,
        inv.NO_OF_DAYS_OUT_OF_STOCK,
        inv.INVENTORY_SERVICE_COSTS,
        inv.INVENTORY_RISK_COSTS,
        inv.STORAGE_COST,
        inv.INVENTORY_CARRYING_COST,
        COALESCE(
            TRY_TO_DATE(TO_VARCHAR(inv.INVENTORY_UPDATE_DATE), 'DD-MM-YYYY'),
            TRY_TO_DATE(TO_VARCHAR(inv.INVENTORY_UPDATE_DATE), 'YYYY-MM-DD'),
            TRY_TO_DATE(inv.INVENTORY_UPDATE_DATE)
        ) AS INVENTORY_UPDATE_DATE
    FROM AI_ETL.AI_ETL_STG.STG_INVENTORY inv
),
inventory_dedup AS (
    SELECT
        inventory_raw.*,
        ROW_NUMBER() OVER (
            PARTITION BY inventory_raw.PRODUCT_ID, inventory_raw.SUPPLIER_ID
            ORDER BY inventory_raw.INVENTORY_UPDATE_DATE DESC
        ) AS rn
    FROM inventory_raw
),
inventory_latest AS (
    SELECT
        PRODUCT_ID,
        SUPPLIER_ID,
        INVENTORY_UNITS,
        IN_OUT_STOCK,
        NO_OF_DAYS_OUT_OF_STOCK,
        INVENTORY_SERVICE_COSTS,
        INVENTORY_RISK_COSTS,
        STORAGE_COST,
        INVENTORY_CARRYING_COST,
        INVENTORY_UPDATE_DATE
    FROM inventory_dedup
    WHERE rn = 1
),
product_raw AS (
    SELECT
        p.PRODUCT_ID,
        p.PRODUCT_NAME,
        p.PRODUCT_COMPONENT,
        p.UNIT_PRICE,
        COALESCE(
            TRY_TO_DATE(TO_VARCHAR(p.EOL_DATE), 'DD-MM-YYYY'),
            TRY_TO_DATE(TO_VARCHAR(p.EOL_DATE), 'YYYY-MM-DD'),
            TRY_TO_DATE(p.EOL_DATE)
        ) AS PRODUCT_EOL_DATE
    FROM AI_ETL.AI_ETL_STG.STG_PRODUCT p
),
product_dedup AS (
    SELECT
        product_raw.*,
        ROW_NUMBER() OVER (
            PARTITION BY product_raw.PRODUCT_ID
            ORDER BY product_raw.PRODUCT_EOL_DATE DESC
        ) AS rn
    FROM product_raw
),
product_latest AS (
    SELECT
        PRODUCT_ID,
        PRODUCT_NAME,
        PRODUCT_COMPONENT,
        UNIT_PRICE,
        PRODUCT_EOL_DATE
    FROM product_dedup
    WHERE rn = 1
),
supplier_raw AS (
    SELECT
        s.SUPPLIER_ID,
        s.SUPPLIER_NAME,
        s.SUPPLIER_CITY,
        s.STATE AS SUPPLIER_STATE,
        s.COUNTRY AS SUPPLIER_COUNTRY,
        s.ROAD_TRANSPORT_COST,
        s.SEA_TRANSPORT_COST,
        s.AIR_TRANSPORT_COST
    FROM AI_ETL.AI_ETL_STG.STG_SUPPLIER s
),
supplier_dedup AS (
    SELECT
        supplier_raw.*,
        ROW_NUMBER() OVER (
            PARTITION BY supplier_raw.SUPPLIER_ID
            ORDER BY supplier_raw.SUPPLIER_NAME ASC
        ) AS rn
    FROM supplier_raw
),
supplier_latest AS (
    SELECT
        SUPPLIER_ID,
        SUPPLIER_NAME,
        SUPPLIER_CITY,
        SUPPLIER_STATE,
        SUPPLIER_COUNTRY,
        ROAD_TRANSPORT_COST,
        SEA_TRANSPORT_COST,
        AIR_TRANSPORT_COST
    FROM supplier_dedup
    WHERE rn = 1
),
customer_raw AS (
    SELECT
        c.CUSTOMERID,
        c.CUSTOMER_NAME,
        c.CUSTOMER_CITY,
        c.CUSTOMER_STATE,
        c.CUSTOMER_COUNTRY
    FROM AI_ETL.AI_ETL_STG.STG_CUSTOMER c
),
customer_dedup AS (
    SELECT
        customer_raw.*,
        ROW_NUMBER() OVER (
            PARTITION BY customer_raw.CUSTOMERID
            ORDER BY customer_raw.CUSTOMER_NAME ASC
        ) AS rn
    FROM customer_raw
),
customer_latest AS (
    SELECT
        CUSTOMERID,
        CUSTOMER_NAME,
        CUSTOMER_CITY,
        CUSTOMER_STATE,
        CUSTOMER_COUNTRY
    FROM customer_dedup
    WHERE rn = 1
),
assembled AS (
    SELECT
        ol.SALES_ORDER_NO,
        ol.PRODUCTID,
        ol.SUPPLIERID,
        ol.CUSTOMERID,
        cust.CUSTOMER_NAME,
        cust.CUSTOMER_CITY,
        cust.CUSTOMER_STATE,
        cust.CUSTOMER_COUNTRY,
        ol.ORDER_DATE,
        ol.EXPECTED_DELIVERY_DATE,
        ol.ACTUAL_DELIVERY_DATE,
        ol.SALES_QUANTITY,
        ol.ACCURATE_ORDER,
        ol.RETURN_FLAG,
        ol.DEFECTIVE_UNIT,
        ol.DEFECT_COUNT,
        ol.MODE_OF_ORDER,
        ol.MODE_OF_SHIPPING,
        ol.SHIPPING_COST,
        ol.ON_TIME_SHIPPING_FLAG,
        ol.ON_TIME_DELIVERY,
        ol.SELLING_PRICE,
        ol.SALES_QUANTITY__BY_SELLING_PRICE,
        ol.DISCOUNT_PER,
        ol.FINAL_PRICE,
        ol.CUSTOMER_SEGMENT,
        pr.PRODUCT_NAME,
        pr.PRODUCT_COMPONENT,
        pr.UNIT_PRICE,
        pr.PRODUCT_EOL_DATE,
        sup.SUPPLIER_NAME,
        sup.SUPPLIER_CITY,
        sup.SUPPLIER_STATE,
        sup.SUPPLIER_COUNTRY,
        inv.INVENTORY_UNITS,
        inv.IN_OUT_STOCK,
        inv.NO_OF_DAYS_OUT_OF_STOCK,
        inv.INVENTORY_SERVICE_COSTS,
        inv.INVENTORY_RISK_COSTS,
        inv.STORAGE_COST,
        inv.INVENTORY_CARRYING_COST,
        inv.INVENTORY_UPDATE_DATE,
        CASE
            WHEN UPPER(ol.MODE_OF_SHIPPING) = 'AIR' THEN COALESCE(sup.AIR_TRANSPORT_COST, 0)
            WHEN UPPER(ol.MODE_OF_SHIPPING) = 'SEA' THEN COALESCE(sup.SEA_TRANSPORT_COST, 0)
            WHEN UPPER(ol.MODE_OF_SHIPPING) = 'ROAD' THEN COALESCE(sup.ROAD_TRANSPORT_COST, 0)
            ELSE 0
        END AS SHIPPING_FACTOR
    FROM orders_latest ol
    LEFT JOIN inventory_latest inv
        ON ol.PRODUCTID = inv.PRODUCT_ID
       AND ol.SUPPLIERID = inv.SUPPLIER_ID
    LEFT JOIN product_latest pr
        ON ol.PRODUCTID = pr.PRODUCT_ID
    LEFT JOIN supplier_latest sup
        ON ol.SUPPLIERID = sup.SUPPLIER_ID
    LEFT JOIN customer_latest cust
        ON ol.CUSTOMERID = cust.CUSTOMERID
),
final_ranked AS (
    SELECT
        assembled.SALES_ORDER_NO,
        assembled.PRODUCTID,
        assembled.SUPPLIERID,
        assembled.CUSTOMERID,
        assembled.CUSTOMER_NAME,
        assembled.CUSTOMER_CITY,
        assembled.CUSTOMER_STATE,
        assembled.CUSTOMER_COUNTRY,
        assembled.ORDER_DATE,
        assembled.EXPECTED_DELIVERY_DATE,
        assembled.ACTUAL_DELIVERY_DATE,
        assembled.SALES_QUANTITY,
        assembled.ACCURATE_ORDER,
        assembled.RETURN_FLAG,
        assembled.DEFECTIVE_UNIT,
        assembled.DEFECT_COUNT,
        assembled.MODE_OF_ORDER,
        assembled.MODE_OF_SHIPPING,
        assembled.SHIPPING_COST,
        assembled.ON_TIME_SHIPPING_FLAG,
        assembled.ON_TIME_DELIVERY,
        assembled.SELLING_PRICE,
        assembled.SALES_QUANTITY__BY_SELLING_PRICE,
        assembled.DISCOUNT_PER,
        assembled.FINAL_PRICE,
        assembled.CUSTOMER_SEGMENT,
        assembled.PRODUCT_NAME,
        assembled.PRODUCT_COMPONENT,
        assembled.UNIT_PRICE,
        assembled.PRODUCT_EOL_DATE,
        assembled.SUPPLIER_NAME,
        assembled.SUPPLIER_CITY,
        assembled.SUPPLIER_STATE,
        assembled.SUPPLIER_COUNTRY,
        assembled.INVENTORY_UNITS,
        assembled.IN_OUT_STOCK,
        assembled.NO_OF_DAYS_OUT_OF_STOCK,
        assembled.INVENTORY_SERVICE_COSTS,
        assembled.INVENTORY_RISK_COSTS,
        assembled.STORAGE_COST,
        assembled.INVENTORY_CARRYING_COST,
        assembled.INVENTORY_UPDATE_DATE,
        assembled.SHIPPING_FACTOR,
        CASE WHEN assembled.ON_TIME_DELIVERY IS TRUE AND assembled.ACCURATE_ORDER IS TRUE THEN 1 ELSE 0 END AS OTIF_FLAG,
        COALESCE(assembled.UNIT_PRICE, 0) + COALESCE(assembled.SHIPPING_COST, 0) + COALESCE(assembled.INVENTORY_SERVICE_COSTS, 0) + COALESCE(assembled.STORAGE_COST, 0) AS LANDED_COST_PER_UNIT,
        CASE WHEN assembled.SALES_QUANTITY IS NOT NULL AND assembled.SALES_QUANTITY <> 0 THEN COALESCE(assembled.INVENTORY_UNITS, 0) / assembled.SALES_QUANTITY ELSE NULL END AS DSI_ESTIMATE,
        CASE
            WHEN COALESCE(assembled.UNIT_PRICE, 0) * COALESCE(assembled.INVENTORY_UNITS, 0) <> 0
            THEN ((COALESCE(assembled.FINAL_PRICE, 0) - COALESCE(assembled.UNIT_PRICE, 0)) * COALESCE(assembled.SALES_QUANTITY, 0)) / (COALESCE(assembled.UNIT_PRICE, 0) * COALESCE(assembled.INVENTORY_UNITS, 0))
            ELSE NULL
        END AS GMROI,
        CASE
            WHEN assembled.ORDER_DATE IS NOT NULL AND assembled.ACTUAL_DELIVERY_DATE IS NOT NULL
            THEN DATEDIFF('DAY', assembled.ORDER_DATE, assembled.ACTUAL_DELIVERY_DATE)
            ELSE NULL
        END AS ORDER_TO_CASH_CYCLE_DAYS,
        CASE
            WHEN assembled.SALES_QUANTITY IS NOT NULL AND assembled.SALES_QUANTITY <> 0
            THEN (CASE WHEN assembled.RETURN_FLAG IS TRUE THEN 1 ELSE 0 END) / assembled.SALES_QUANTITY
            ELSE NULL
        END AS RETURN_TO_SALES_RATIO,
        CASE
            WHEN assembled.SALES_QUANTITY IS NOT NULL AND assembled.SALES_QUANTITY <> 0
            THEN COALESCE(assembled.SHIPPING_COST, 0) / assembled.SALES_QUANTITY
            ELSE NULL
        END AS LAST_MILE_EFFICIENCY,
        CASE
            WHEN assembled.SALES_QUANTITY IS NOT NULL AND assembled.SALES_QUANTITY <> 0
            THEN COALESCE(assembled.NO_OF_DAYS_OUT_OF_STOCK, 0) / assembled.SALES_QUANTITY
            ELSE NULL
        END AS STOCKOUT_RISK_SCORE,
        CASE
            WHEN assembled.SALES_QUANTITY IS NOT NULL
            THEN assembled.SALES_QUANTITY * COALESCE(assembled.SHIPPING_FACTOR, 0)
            ELSE NULL
        END AS CARBON_FOOTPRINT_EST,
        CASE
            WHEN assembled.EXPECTED_DELIVERY_DATE IS NOT NULL AND assembled.ACTUAL_DELIVERY_DATE IS NOT NULL
            THEN DATEDIFF('DAY', assembled.EXPECTED_DELIVERY_DATE, assembled.ACTUAL_DELIVERY_DATE)
            ELSE NULL
        END AS CARRIER_LEAD_TIME_VARIANCE,
        ROW_NUMBER() OVER (
            PARTITION BY assembled.SALES_ORDER_NO, assembled.PRODUCTID, assembled.SUPPLIERID
            ORDER BY assembled.ORDER_DATE DESC, assembled.INVENTORY_UPDATE_DATE DESC, assembled.FINAL_PRICE DESC
        ) AS rn
    FROM assembled
)
SELECT
    final_ranked.SALES_ORDER_NO,
    final_ranked.PRODUCTID,
    final_ranked.SUPPLIERID,
    final_ranked.CUSTOMERID,
    final_ranked.CUSTOMER_NAME,
    final_ranked.CUSTOMER_CITY,
    final_ranked.CUSTOMER_STATE,
    final_ranked.CUSTOMER_COUNTRY,
    final_ranked.ORDER_DATE,
    final_ranked.EXPECTED_DELIVERY_DATE,
    final_ranked.ACTUAL_DELIVERY_DATE,
    final_ranked.SALES_QUANTITY,
    final_ranked.ACCURATE_ORDER,
    final_ranked.RETURN_FLAG,
    final_ranked.DEFECTIVE_UNIT,
    final_ranked.DEFECT_COUNT,
    final_ranked.MODE_OF_ORDER,
    final_ranked.MODE_OF_SHIPPING,
    final_ranked.SHIPPING_COST,
    final_ranked.ON_TIME_SHIPPING_FLAG,
    final_ranked.ON_TIME_DELIVERY,
    final_ranked.SELLING_PRICE,
    final_ranked.SALES_QUANTITY__BY_SELLING_PRICE,
    final_ranked.DISCOUNT_PER,
    final_ranked.FINAL_PRICE,
    final_ranked.CUSTOMER_SEGMENT,
    final_ranked.PRODUCT_NAME,
    final_ranked.PRODUCT_COMPONENT,
    final_ranked.UNIT_PRICE,
    final_ranked.PRODUCT_EOL_DATE,
    final_ranked.SUPPLIER_NAME,
    final_ranked.SUPPLIER_CITY,
    final_ranked.SUPPLIER_STATE,
    final_ranked.SUPPLIER_COUNTRY,
    final_ranked.INVENTORY_UNITS,
    final_ranked.IN_OUT_STOCK,
    final_ranked.NO_OF_DAYS_OUT_OF_STOCK,
    final_ranked.INVENTORY_SERVICE_COSTS,
    final_ranked.INVENTORY_RISK_COSTS,
    final_ranked.STORAGE_COST,
    final_ranked.INVENTORY_CARRYING_COST,
    final_ranked.INVENTORY_UPDATE_DATE,
    final_ranked.SHIPPING_FACTOR,
    final_ranked.OTIF_FLAG,
    final_ranked.LANDED_COST_PER_UNIT,
    final_ranked.DSI_ESTIMATE,
    final_ranked.GMROI,
    final_ranked.ORDER_TO_CASH_CYCLE_DAYS,
    final_ranked.RETURN_TO_SALES_RATIO,
    final_ranked.LAST_MILE_EFFICIENCY,
    final_ranked.STOCKOUT_RISK_SCORE,
    final_ranked.CARBON_FOOTPRINT_EST,
    final_ranked.CARRIER_LEAD_TIME_VARIANCE
FROM final_ranked
WHERE final_ranked.rn = 1
) S
ON (
    T.SALES_ORDER_NO = S.SALES_ORDER_NO
    AND T.PRODUCTID = S.PRODUCTID
    AND T.SUPPLIERID = S.SUPPLIERID
)
WHEN MATCHED THEN UPDATE SET
    T.CUSTOMERID = S.CUSTOMERID,
    T.CUSTOMER_NAME = S.CUSTOMER_NAME,
    T.CUSTOMER_CITY = S.CUSTOMER_CITY,
    T.CUSTOMER_STATE = S.CUSTOMER_STATE,
    T.CUSTOMER_COUNTRY = S.CUSTOMER_COUNTRY,
    T.ORDER_DATE = S.ORDER_DATE,
    T.EXPECTED_DELIVERY_DATE = S.EXPECTED_DELIVERY_DATE,
    T.ACTUAL_DELIVERY_DATE = S.ACTUAL_DELIVERY_DATE,
    T.SALES_QUANTITY = S.SALES_QUANTITY,
    T.ACCURATE_ORDER = S.ACCURATE_ORDER,
    T.RETURN_FLAG = S.RETURN_FLAG,
    T.DEFECTIVE_UNIT = S.DEFECTIVE_UNIT,
    T.DEFECT_COUNT = S.DEFECT_COUNT,
    T.MODE_OF_ORDER = S.MODE_OF_ORDER,
    T.MODE_OF_SHIPPING = S.MODE_OF_SHIPPING,
    T.SHIPPING_COST = S.SHIPPING_COST,
    T.ON_TIME_SHIPPING_FLAG = S.ON_TIME_SHIPPING_FLAG,
    T.ON_TIME_DELIVERY = S.ON_TIME_DELIVERY,
    T.SELLING_PRICE = S.SELLING_PRICE,
    T.SALES_QUANTITY__BY_SELLING_PRICE = S.SALES_QUANTITY__BY_SELLING_PRICE,
    T.DISCOUNT_PER = S.DISCOUNT_PER,
    T.FINAL_PRICE = S.FINAL_PRICE,
    T.CUSTOMER_SEGMENT = S.CUSTOMER_SEGMENT,
    T.PRODUCT_NAME = S.PRODUCT_NAME,
    T.PRODUCT_COMPONENT = S.PRODUCT_COMPONENT,
    T.UNIT_PRICE = S.UNIT_PRICE,
    T.PRODUCT_EOL_DATE = S.PRODUCT_EOL_DATE,
    T.SUPPLIER_NAME = S.SUPPLIER_NAME,
    T.SUPPLIER_CITY = S.SUPPLIER_CITY,
    T.SUPPLIER_STATE = S.SUPPLIER_STATE,
    T.SUPPLIER_COUNTRY = S.SUPPLIER_COUNTRY,
    T.INVENTORY_UNITS = S.INVENTORY_UNITS,
    T.IN_OUT_STOCK = S.IN_OUT_STOCK,
    T.NO_OF_DAYS_OUT_OF_STOCK = S.NO_OF_DAYS_OUT_OF_STOCK,
    T.INVENTORY_SERVICE_COSTS = S.INVENTORY_SERVICE_COSTS,
    T.INVENTORY_RISK_COSTS = S.INVENTORY_RISK_COSTS,
    T.STORAGE_COST = S.STORAGE_COST,
    T.INVENTORY_CARRYING_COST = S.INVENTORY_CARRYING_COST,
    T.INVENTORY_UPDATE_DATE = S.INVENTORY_UPDATE_DATE,
    T.SHIPPING_FACTOR = S.SHIPPING_FACTOR,
    T.OTIF_FLAG = S.OTIF_FLAG,
    T.LANDED_COST_PER_UNIT = S.LANDED_COST_PER_UNIT,
    T.DSI_ESTIMATE = S.DSI_ESTIMATE,
    T.GMROI = S.GMROI,
    T.ORDER_TO_CASH_CYCLE_DAYS = S.ORDER_TO_CASH_CYCLE_DAYS,
    T.RETURN_TO_SALES_RATIO = S.RETURN_TO_SALES_RATIO,
    T.LAST_MILE_EFFICIENCY = S.LAST_MILE_EFFICIENCY,
    T.STOCKOUT_RISK_SCORE = S.STOCKOUT_RISK_SCORE,
    T.CARBON_FOOTPRINT_EST = S.CARBON_FOOTPRINT_EST,
    T.CARRIER_LEAD_TIME_VARIANCE = S.CARRIER_LEAD_TIME_VARIANCE
WHEN NOT MATCHED THEN INSERT (
    SALES_ORDER_NO,
    PRODUCTID,
    SUPPLIERID,
    CUSTOMERID,
    CUSTOMER_NAME,
    CUSTOMER_CITY,
    CUSTOMER_STATE,
    CUSTOMER_COUNTRY,
    ORDER_DATE,
    EXPECTED_DELIVERY_DATE,
    ACTUAL_DELIVERY_DATE,
    SALES_QUANTITY,
    ACCURATE_ORDER,
    RETURN_FLAG,
    DEFECTIVE_UNIT,
    DEFECT_COUNT,
    MODE_OF_ORDER,
    MODE_OF_SHIPPING,
    SHIPPING_COST,
    ON_TIME_SHIPPING_FLAG,
    ON_TIME_DELIVERY,
    SELLING_PRICE,
    SALES_QUANTITY__BY_SELLING_PRICE,
    DISCOUNT_PER,
    FINAL_PRICE,
    CUSTOMER_SEGMENT,
    PRODUCT_NAME,
    PRODUCT_COMPONENT,
    UNIT_PRICE,
    PRODUCT_EOL_DATE,
    SUPPLIER_NAME,
    SUPPLIER_CITY,
    SUPPLIER_STATE,
    SUPPLIER_COUNTRY,
    INVENTORY_UNITS,
    IN_OUT_STOCK,
    NO_OF_DAYS_OUT_OF_STOCK,
    INVENTORY_SERVICE_COSTS,
    INVENTORY_RISK_COSTS,
    STORAGE_COST,
    INVENTORY_CARRYING_COST,
    INVENTORY_UPDATE_DATE,
    SHIPPING_FACTOR,
    OTIF_FLAG,
    LANDED_COST_PER_UNIT,
    DSI_ESTIMATE,
    GMROI,
    ORDER_TO_CASH_CYCLE_DAYS,
    RETURN_TO_SALES_RATIO,
    LAST_MILE_EFFICIENCY,
    STOCKOUT_RISK_SCORE,
    CARBON_FOOTPRINT_EST,
    CARRIER_LEAD_TIME_VARIANCE
)
VALUES (
    S.SALES_ORDER_NO,
    S.PRODUCTID,
    S.SUPPLIERID,
    S.CUSTOMERID,
    S.CUSTOMER_NAME,
    S.CUSTOMER_CITY,
    S.CUSTOMER_STATE,
    S.CUSTOMER_COUNTRY,
    S.ORDER_DATE,
    S.EXPECTED_DELIVERY_DATE,
    S.ACTUAL_DELIVERY_DATE,
    S.SALES_QUANTITY,
    S.ACCURATE_ORDER,
    S.RETURN_FLAG,
    S.DEFECTIVE_UNIT,
    S.DEFECT_COUNT,
    S.MODE_OF_ORDER,
    S.MODE_OF_SHIPPING,
    S.SHIPPING_COST,
    S.ON_TIME_SHIPPING_FLAG,
    S.ON_TIME_DELIVERY,
    S.SELLING_PRICE,
    S.SALES_QUANTITY__BY_SELLING_PRICE,
    S.DISCOUNT_PER,
    S.FINAL_PRICE,
    S.CUSTOMER_SEGMENT,
    S.PRODUCT_NAME,
    S.PRODUCT_COMPONENT,
    S.UNIT_PRICE,
    S.PRODUCT_EOL_DATE,
    S.SUPPLIER_NAME,
    S.SUPPLIER_CITY,
    S.SUPPLIER_STATE,
    S.SUPPLIER_COUNTRY,
    S.INVENTORY_UNITS,
    S.IN_OUT_STOCK,
    S.NO_OF_DAYS_OUT_OF_STOCK,
    S.INVENTORY_SERVICE_COSTS,
    S.INVENTORY_RISK_COSTS,
    S.STORAGE_COST,
    S.INVENTORY_CARRYING_COST,
    S.INVENTORY_UPDATE_DATE,
    S.SHIPPING_FACTOR,
    S.OTIF_FLAG,
    S.LANDED_COST_PER_UNIT,
    S.DSI_ESTIMATE,
    S.GMROI,
    S.ORDER_TO_CASH_CYCLE_DAYS,
    S.RETURN_TO_SALES_RATIO,
    S.LAST_MILE_EFFICIENCY,
    S.STOCKOUT_RISK_SCORE,
    S.CARBON_FOOTPRINT_EST,
    S.CARRIER_LEAD_TIME_VARIANCE
);