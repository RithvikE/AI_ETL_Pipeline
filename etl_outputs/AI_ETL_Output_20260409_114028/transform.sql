CREATE TABLE IF NOT EXISTS AI_ETL.AI_ETL_WI.TRF_ORDER_SUPPLY_METRICS AS
WITH orders_parsed AS (
    SELECT
        SALES_ORDER_NO,
        PRODUCTID,
        SUPPLIERID,
        CUSTOMERID,
        COALESCE(
            TRY_TO_DATE(TO_VARCHAR("DATE"), 'DD-MM-YYYY'),
            TRY_TO_DATE(TO_VARCHAR("DATE"), 'YYYY-MM-DD'),
            TRY_TO_DATE("DATE")
        ) AS order_date,
        SALES_QUANTITY,
        ACCURATE_ORDER,
        RETURN AS return_flag,
        SHIPPING_COST,
        ON_TIME_DELIVERY,
        FINAL_PRICE,
        MODE_OF_ORDER,
        MODE_OF_SHIPPING,
        DISCOUNT_PER,
        SELLING_PRICE,
        COALESCE(
            TRY_TO_DATE(TO_VARCHAR(EXPECTED_DELIVERY_DATE), 'DD-MM-YYYY'),
            TRY_TO_DATE(TO_VARCHAR(EXPECTED_DELIVERY_DATE), 'YYYY-MM-DD'),
            TRY_TO_DATE(EXPECTED_DELIVERY_DATE)
        ) AS expected_delivery_date,
        COALESCE(
            TRY_TO_DATE(TO_VARCHAR(ACTUAL_DELIVERY_DATE), 'DD-MM-YYYY'),
            TRY_TO_DATE(TO_VARCHAR(ACTUAL_DELIVERY_DATE), 'YYYY-MM-DD'),
            TRY_TO_DATE(ACTUAL_DELIVERY_DATE)
        ) AS actual_delivery_date,
        ON_TIME_SHIPPING_FLAG,
        SALES_QUANTITY__BY_SELLING_PRICE
    FROM AI_ETL.AI_ETL_STG.STG_ORDERS
),
orders_dedup AS (
    SELECT
        SALES_ORDER_NO,
        PRODUCTID,
        SUPPLIERID,
        CUSTOMERID,
        order_date,
        SALES_QUANTITY,
        ACCURATE_ORDER,
        return_flag,
        SHIPPING_COST,
        ON_TIME_DELIVERY,
        FINAL_PRICE,
        MODE_OF_ORDER,
        MODE_OF_SHIPPING,
        DISCOUNT_PER,
        SELLING_PRICE,
        expected_delivery_date,
        actual_delivery_date,
        ON_TIME_SHIPPING_FLAG,
        SALES_QUANTITY__BY_SELLING_PRICE,
        ROW_NUMBER() OVER (
            PARTITION BY SALES_ORDER_NO, PRODUCTID, SUPPLIERID
            ORDER BY order_date DESC NULLS LAST
        ) AS rn
    FROM orders_parsed
),
orders_latest AS (
    SELECT
        SALES_ORDER_NO,
        PRODUCTID,
        SUPPLIERID,
        CUSTOMERID,
        order_date,
        SALES_QUANTITY,
        ACCURATE_ORDER,
        return_flag,
        SHIPPING_COST,
        ON_TIME_DELIVERY,
        FINAL_PRICE,
        MODE_OF_ORDER,
        MODE_OF_SHIPPING,
        DISCOUNT_PER,
        SELLING_PRICE,
        expected_delivery_date,
        actual_delivery_date,
        ON_TIME_SHIPPING_FLAG,
        SALES_QUANTITY__BY_SELLING_PRICE
    FROM orders_dedup
    WHERE rn = 1
),
inventory_parsed AS (
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
        COALESCE(
            TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR(INVENTORY_UPDATE_DATE), 'YYYY-MM-DD HH24:MI:SS'),
            TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR(INVENTORY_UPDATE_DATE), 'YYYY-MM-DD'),
            TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR(INVENTORY_UPDATE_DATE), 'DD-MM-YYYY'),
            TRY_TO_TIMESTAMP_NTZ(INVENTORY_UPDATE_DATE)
        ) AS inventory_update_ts
    FROM AI_ETL.AI_ETL_STG.STG_INVENTORY
),
inventory_dedup AS (
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
        inventory_update_ts,
        ROW_NUMBER() OVER (
            PARTITION BY PRODUCT_ID, SUPPLIER_ID
            ORDER BY inventory_update_ts DESC NULLS LAST
        ) AS rn
    FROM inventory_parsed
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
        inventory_update_ts
    FROM inventory_dedup
    WHERE rn = 1
),
product_clean AS (
    SELECT
        PRODUCT_ID,
        PRODUCT_NAME,
        PRODUCT_COMPONENT,
        UNIT_PRICE,
        COALESCE(
            TRY_TO_DATE(TO_VARCHAR(EOL_DATE), 'DD-MM-YYYY'),
            TRY_TO_DATE(TO_VARCHAR(EOL_DATE), 'YYYY-MM-DD'),
            TRY_TO_DATE(EOL_DATE)
        ) AS eol_date
    FROM AI_ETL.AI_ETL_STG.STG_PRODUCT
),
supplier_clean AS (
    SELECT
        SUPPLIER_ID,
        SUPPLIER_NAME,
        SUPPLIER_CITY,
        STATE AS supplier_state,
        COUNTRY AS supplier_country,
        PHONE AS supplier_phone,
        EMAIL AS supplier_email,
        RISK AS supplier_risk,
        ROAD_TRANSPORT_COST,
        SEA_TRANSPORT_COST,
        AIR_TRANSPORT_COST
    FROM AI_ETL.AI_ETL_STG.STG_SUPPLIER
),
customer_clean AS (
    SELECT
        CUSTOMERID,
        CUSTOMER_NAME,
        CUSTOMER_CITY,
        CUSTOMER_STATE,
        CUSTOMER_COUNTRY,
        CUSTOMER_COUNTRY_CODE,
        CUSTOMERSTATE_CODE,
        PHONE AS customer_phone,
        EMAIL AS customer_email
    FROM AI_ETL.AI_ETL_STG.STG_CUSTOMER
),
final_base AS (
    SELECT
        o.SALES_ORDER_NO,
        o.PRODUCTID,
        o.SUPPLIERID,
        o.CUSTOMERID,
        o.order_date,
        o.expected_delivery_date,
        o.actual_delivery_date,
        o.SALES_QUANTITY,
        o.ACCURATE_ORDER,
        o.return_flag,
        o.SHIPPING_COST,
        o.ON_TIME_DELIVERY,
        o.FINAL_PRICE,
        o.MODE_OF_ORDER,
        o.MODE_OF_SHIPPING,
        o.DISCOUNT_PER,
        o.SELLING_PRICE,
        o.ON_TIME_SHIPPING_FLAG,
        o.SALES_QUANTITY__BY_SELLING_PRICE,
        c.CUSTOMER_NAME,
        c.CUSTOMER_CITY,
        c.CUSTOMER_STATE,
        c.CUSTOMER_COUNTRY,
        c.CUSTOMER_COUNTRY_CODE,
        c.CUSTOMERSTATE_CODE,
        c.customer_phone,
        c.customer_email,
        s.SUPPLIER_NAME,
        s.SUPPLIER_CITY,
        s.supplier_state,
        s.supplier_country,
        s.supplier_phone,
        s.supplier_email,
        s.supplier_risk,
        s.ROAD_TRANSPORT_COST,
        s.SEA_TRANSPORT_COST,
        s.AIR_TRANSPORT_COST,
        p.PRODUCT_NAME,
        p.PRODUCT_COMPONENT,
        p.UNIT_PRICE,
        p.eol_date,
        i.INVENTORY_UNITS,
        i.IN_OUT_STOCK,
        i.NO_OF_DAYS_OUT_OF_STOCK,
        i.INVENTORY_SERVICE_COSTS,
        i.INVENTORY_RISK_COSTS,
        i.STORAGE_COST,
        i.INVENTORY_CARRYING_COST,
        i.inventory_update_ts,
        CASE
            WHEN UPPER(o.MODE_OF_SHIPPING) = 'AIR' THEN COALESCE(s.AIR_TRANSPORT_COST, 0)
            WHEN UPPER(o.MODE_OF_SHIPPING) = 'SEA' THEN COALESCE(s.SEA_TRANSPORT_COST, 0)
            WHEN UPPER(o.MODE_OF_SHIPPING) = 'ROAD' THEN COALESCE(s.ROAD_TRANSPORT_COST, 0)
            ELSE COALESCE(s.ROAD_TRANSPORT_COST, COALESCE(s.SEA_TRANSPORT_COST, s.AIR_TRANSPORT_COST, 0))
        END AS shipping_factor_value
    FROM orders_latest o
    LEFT JOIN inventory_latest i
        ON o.PRODUCTID = i.PRODUCT_ID
       AND o.SUPPLIERID = i.SUPPLIER_ID
    LEFT JOIN product_clean p
        ON o.PRODUCTID = p.PRODUCT_ID
    LEFT JOIN supplier_clean s
        ON o.SUPPLIERID = s.SUPPLIER_ID
    LEFT JOIN customer_clean c
        ON o.CUSTOMERID = c.CUSTOMERID
),
final_enriched AS (
    SELECT
        final_base.SALES_ORDER_NO,
        final_base.PRODUCTID,
        final_base.SUPPLIERID,
        final_base.CUSTOMERID,
        final_base.order_date,
        final_base.expected_delivery_date,
        final_base.actual_delivery_date,
        final_base.SALES_QUANTITY,
        final_base.ACCURATE_ORDER,
        final_base.return_flag,
        final_base.SHIPPING_COST,
        final_base.ON_TIME_DELIVERY,
        final_base.FINAL_PRICE,
        final_base.MODE_OF_ORDER,
        final_base.MODE_OF_SHIPPING,
        final_base.DISCOUNT_PER,
        final_base.SELLING_PRICE,
        final_base.ON_TIME_SHIPPING_FLAG,
        final_base.SALES_QUANTITY__BY_SELLING_PRICE,
        final_base.CUSTOMER_NAME,
        final_base.CUSTOMER_CITY,
        final_base.CUSTOMER_STATE,
        final_base.CUSTOMER_COUNTRY,
        final_base.CUSTOMER_COUNTRY_CODE,
        final_base.CUSTOMERSTATE_CODE,
        final_base.customer_phone,
        final_base.customer_email,
        final_base.SUPPLIER_NAME,
        final_base.SUPPLIER_CITY,
        final_base.supplier_state,
        final_base.supplier_country,
        final_base.supplier_phone,
        final_base.supplier_email,
        final_base.supplier_risk,
        final_base.ROAD_TRANSPORT_COST,
        final_base.SEA_TRANSPORT_COST,
        final_base.AIR_TRANSPORT_COST,
        final_base.PRODUCT_NAME,
        final_base.PRODUCT_COMPONENT,
        final_base.UNIT_PRICE,
        final_base.eol_date,
        final_base.INVENTORY_UNITS,
        final_base.IN_OUT_STOCK,
        final_base.NO_OF_DAYS_OUT_OF_STOCK,
        final_base.INVENTORY_SERVICE_COSTS,
        final_base.INVENTORY_RISK_COSTS,
        final_base.STORAGE_COST,
        final_base.INVENTORY_CARRYING_COST,
        final_base.inventory_update_ts,
        final_base.shipping_factor_value AS shipping_factor,
        CASE WHEN final_base.return_flag = TRUE THEN 1 ELSE 0 END AS return_indicator,
        CASE WHEN final_base.ON_TIME_DELIVERY = TRUE AND final_base.ACCURATE_ORDER = TRUE THEN 1 ELSE 0 END AS otif_flag
    FROM final_base
),
final_metrics AS (
    SELECT
        final_enriched.SALES_ORDER_NO,
        final_enriched.PRODUCTID,
        final_enriched.SUPPLIERID,
        final_enriched.CUSTOMERID,
        final_enriched.order_date,
        final_enriched.expected_delivery_date,
        final_enriched.actual_delivery_date,
        final_enriched.SALES_QUANTITY,
        final_enriched.ACCURATE_ORDER,
        final_enriched.return_flag,
        final_enriched.return_indicator,
        final_enriched.SHIPPING_COST,
        final_enriched.ON_TIME_DELIVERY,
        final_enriched.FINAL_PRICE,
        final_enriched.MODE_OF_ORDER,
        final_enriched.MODE_OF_SHIPPING,
        final_enriched.DISCOUNT_PER,
        final_enriched.SELLING_PRICE,
        final_enriched.ON_TIME_SHIPPING_FLAG,
        final_enriched.SALES_QUANTITY__BY_SELLING_PRICE,
        final_enriched.CUSTOMER_NAME,
        final_enriched.CUSTOMER_CITY,
        final_enriched.CUSTOMER_STATE,
        final_enriched.CUSTOMER_COUNTRY,
        final_enriched.CUSTOMER_COUNTRY_CODE,
        final_enriched.CUSTOMERSTATE_CODE,
        final_enriched.customer_phone,
        final_enriched.customer_email,
        final_enriched.SUPPLIER_NAME,
        final_enriched.SUPPLIER_CITY,
        final_enriched.supplier_state,
        final_enriched.supplier_country,
        final_enriched.supplier_phone,
        final_enriched.supplier_email,
        final_enriched.supplier_risk,
        final_enriched.ROAD_TRANSPORT_COST,
        final_enriched.SEA_TRANSPORT_COST,
        final_enriched.AIR_TRANSPORT_COST,
        final_enriched.PRODUCT_NAME,
        final_enriched.PRODUCT_COMPONENT,
        final_enriched.UNIT_PRICE,
        final_enriched.eol_date,
        final_enriched.INVENTORY_UNITS,
        final_enriched.IN_OUT_STOCK,
        final_enriched.NO_OF_DAYS_OUT_OF_STOCK,
        final_enriched.INVENTORY_SERVICE_COSTS,
        final_enriched.INVENTORY_RISK_COSTS,
        final_enriched.STORAGE_COST,
        final_enriched.INVENTORY_CARRYING_COST,
        final_enriched.inventory_update_ts,
        final_enriched.shipping_factor,
        final_enriched.otif_flag,
        CASE
            WHEN final_enriched.sales_quantity IS NULL OR final_enriched.sales_quantity = 0 THEN NULL
            ELSE final_enriched.INVENTORY_UNITS / final_enriched.sales_quantity
        END AS dsi_estimate,
        COALESCE(final_enriched.UNIT_PRICE, 0) + COALESCE(final_enriched.SHIPPING_COST, 0) + COALESCE(final_enriched.INVENTORY_SERVICE_COSTS, 0) + COALESCE(final_enriched.STORAGE_COST, 0) AS landed_cost_per_unit,
        CASE
            WHEN final_enriched.UNIT_PRICE IS NULL OR final_enriched.INVENTORY_UNITS IS NULL OR final_enriched.UNIT_PRICE = 0 OR final_enriched.INVENTORY_UNITS = 0 THEN NULL
            ELSE ((COALESCE(final_enriched.FINAL_PRICE, 0) - COALESCE(final_enriched.UNIT_PRICE, 0)) * COALESCE(final_enriched.SALES_QUANTITY, 0)) / (final_enriched.UNIT_PRICE * final_enriched.INVENTORY_UNITS)
        END AS gmroi,
        DATEDIFF('day', final_enriched.order_date, final_enriched.actual_delivery_date) AS order_to_cash_cycle_days,
        CASE
            WHEN final_enriched.sales_quantity IS NULL OR final_enriched.sales_quantity = 0 THEN NULL
            ELSE final_enriched.return_indicator / final_enriched.sales_quantity
        END AS return_to_sales_ratio,
        CASE
            WHEN final_enriched.sales_quantity IS NULL OR final_enriched.sales_quantity = 0 THEN NULL
            ELSE COALESCE(final_enriched.SHIPPING_COST, 0) / final_enriched.sales_quantity
        END AS last_mile_efficiency,
        CASE
            WHEN final_enriched.sales_quantity IS NULL OR final_enriched.sales_quantity = 0 THEN NULL
            ELSE COALESCE(final_enriched.NO_OF_DAYS_OUT_OF_STOCK, 0) / final_enriched.sales_quantity
        END AS stockout_risk_score,
        COALESCE(final_enriched.SALES_QUANTITY, 0) * COALESCE(final_enriched.shipping_factor, 0) AS carbon_footprint_est,
        DATEDIFF('day', final_enriched.expected_delivery_date, final_enriched.actual_delivery_date) AS carrier_lead_time_variance
    FROM final_enriched
),
final_dedup AS (
    SELECT
        final_metrics.*,
        ROW_NUMBER() OVER (
            PARTITION BY final_metrics.SALES_ORDER_NO, final_metrics.PRODUCTID, final_metrics.SUPPLIERID
            ORDER BY final_metrics.order_date DESC NULLS LAST
        ) AS rn
    FROM final_metrics
)
SELECT
    final_dedup.SALES_ORDER_NO AS SALES_ORDER_NO,
    final_dedup.PRODUCTID AS PRODUCTID,
    final_dedup.SUPPLIERID AS SUPPLIERID,
    final_dedup.CUSTOMERID AS CUSTOMERID,
    final_dedup.order_date AS ORDER_DATE,
    final_dedup.expected_delivery_date AS EXPECTED_DELIVERY_DATE,
    final_dedup.actual_delivery_date AS ACTUAL_DELIVERY_DATE,
    final_dedup.SALES_QUANTITY AS SALES_QUANTITY,
    final_dedup.ACCURATE_ORDER AS ACCURATE_ORDER,
    final_dedup.return_flag AS RETURN_FLAG,
    final_dedup.return_indicator AS RETURN_INDICATOR,
    final_dedup.SHIPPING_COST AS SHIPPING_COST,
    final_dedup.ON_TIME_DELIVERY AS ON_TIME_DELIVERY,
    final_dedup.FINAL_PRICE AS FINAL_PRICE,
    final_dedup.MODE_OF_ORDER AS MODE_OF_ORDER,
    final_dedup.MODE_OF_SHIPPING AS MODE_OF_SHIPPING,
    final_dedup.DISCOUNT_PER AS DISCOUNT_PER,
    final_dedup.SELLING_PRICE AS SELLING_PRICE,
    final_dedup.ON_TIME_SHIPPING_FLAG AS ON_TIME_SHIPPING_FLAG,
    final_dedup.SALES_QUANTITY__BY_SELLING_PRICE AS SALES_QUANTITY__BY_SELLING_PRICE,
    final_dedup.CUSTOMER_NAME AS CUSTOMER_NAME,
    final_dedup.CUSTOMER_CITY AS CUSTOMER_CITY,
    final_dedup.CUSTOMER_STATE AS CUSTOMER_STATE,
    final_dedup.CUSTOMER_COUNTRY AS CUSTOMER_COUNTRY,
    final_dedup.CUSTOMER_COUNTRY_CODE AS CUSTOMER_COUNTRY_CODE,
    final_dedup.CUSTOMERSTATE_CODE AS CUSTOMERSTATE_CODE,
    final_dedup.customer_phone AS CUSTOMER_PHONE,
    final_dedup.customer_email AS CUSTOMER_EMAIL,
    final_dedup.SUPPLIER_NAME AS SUPPLIER_NAME,
    final_dedup.SUPPLIER_CITY AS SUPPLIER_CITY,
    final_dedup.supplier_state AS SUPPLIER_STATE,
    final_dedup.supplier_country AS SUPPLIER_COUNTRY,
    final_dedup.supplier_phone AS SUPPLIER_PHONE,
    final_dedup.supplier_email AS SUPPLIER_EMAIL,
    final_dedup.supplier_risk AS SUPPLIER_RISK,
    final_dedup.ROAD_TRANSPORT_COST AS ROAD_TRANSPORT_COST,
    final_dedup.SEA_TRANSPORT_COST AS SEA_TRANSPORT_COST,
    final_dedup.AIR_TRANSPORT_COST AS AIR_TRANSPORT_COST,
    final_dedup.PRODUCT_NAME AS PRODUCT_NAME,
    final_dedup.PRODUCT_COMPONENT AS PRODUCT_COMPONENT,
    final_dedup.UNIT_PRICE AS UNIT_PRICE,
    final_dedup.eol_date AS EOL_DATE,
    final_dedup.INVENTORY_UNITS AS INVENTORY_UNITS,
    final_dedup.IN_OUT_STOCK AS IN_OUT_STOCK,
    final_dedup.NO_OF_DAYS_OUT_OF_STOCK AS NO_OF_DAYS_OUT_OF_STOCK,
    final_dedup.INVENTORY_SERVICE_COSTS AS INVENTORY_SERVICE_COSTS,
    final_dedup.INVENTORY_RISK_COSTS AS INVENTORY_RISK_COSTS,
    final_dedup.STORAGE_COST AS STORAGE_COST,
    final_dedup.INVENTORY_CARRYING_COST AS INVENTORY_CARRYING_COST,
    final_dedup.inventory_update_ts AS INVENTORY_UPDATE_TS,
    final_dedup.shipping_factor AS SHIPPING_FACTOR,
    final_dedup.otif_flag AS OTIF_FLAG,
    final_dedup.landed_cost_per_unit AS LANDED_COST_PER_UNIT,
    final_dedup.dsi_estimate AS DSI_ESTIMATE,
    final_dedup.gmroi AS GMROI,
    final_dedup.order_to_cash_cycle_days AS ORDER_TO_CASH_CYCLE_DAYS,
    final_dedup.return_to_sales_ratio AS RETURN_TO_SALES_RATIO,
    final_dedup.last_mile_efficiency AS LAST_MILE_EFFICIENCY,
    final_dedup.stockout_risk_score AS STOCKOUT_RISK_SCORE,
    final_dedup.carbon_footprint_est AS CARBON_FOOTPRINT_EST,
    final_dedup.carrier_lead_time_variance AS CARRIER_LEAD_TIME_VARIANCE
FROM final_dedup
WHERE rn = 1;

MERGE INTO AI_ETL.AI_ETL_WI.TRF_ORDER_SUPPLY_METRICS AS T
USING (
    WITH orders_parsed AS (
        SELECT
            SALES_ORDER_NO,
            PRODUCTID,
            SUPPLIERID,
            CUSTOMERID,
            COALESCE(
                TRY_TO_DATE(TO_VARCHAR("DATE"), 'DD-MM-YYYY'),
                TRY_TO_DATE(TO_VARCHAR("DATE"), 'YYYY-MM-DD'),
                TRY_TO_DATE("DATE")
            ) AS order_date,
            SALES_QUANTITY,
            ACCURATE_ORDER,
            RETURN AS return_flag,
            SHIPPING_COST,
            ON_TIME_DELIVERY,
            FINAL_PRICE,
            MODE_OF_ORDER,
            MODE_OF_SHIPPING,
            DISCOUNT_PER,
            SELLING_PRICE,
            COALESCE(
                TRY_TO_DATE(TO_VARCHAR(EXPECTED_DELIVERY_DATE), 'DD-MM-YYYY'),
                TRY_TO_DATE(TO_VARCHAR(EXPECTED_DELIVERY_DATE), 'YYYY-MM-DD'),
                TRY_TO_DATE(EXPECTED_DELIVERY_DATE)
            ) AS expected_delivery_date,
            COALESCE(
                TRY_TO_DATE(TO_VARCHAR(ACTUAL_DELIVERY_DATE), 'DD-MM-YYYY'),
                TRY_TO_DATE(TO_VARCHAR(ACTUAL_DELIVERY_DATE), 'YYYY-MM-DD'),
                TRY_TO_DATE(ACTUAL_DELIVERY_DATE)
            ) AS actual_delivery_date,
            ON_TIME_SHIPPING_FLAG,
            SALES_QUANTITY__BY_SELLING_PRICE
        FROM AI_ETL.AI_ETL_STG.STG_ORDERS
    ),
    orders_dedup AS (
        SELECT
            SALES_ORDER_NO,
            PRODUCTID,
            SUPPLIERID,
            CUSTOMERID,
            order_date,
            SALES_QUANTITY,
            ACCURATE_ORDER,
            return_flag,
            SHIPPING_COST,
            ON_TIME_DELIVERY,
            FINAL_PRICE,
            MODE_OF_ORDER,
            MODE_OF_SHIPPING,
            DISCOUNT_PER,
            SELLING_PRICE,
            expected_delivery_date,
            actual_delivery_date,
            ON_TIME_SHIPPING_FLAG,
            SALES_QUANTITY__BY_SELLING_PRICE,
            ROW_NUMBER() OVER (
                PARTITION BY SALES_ORDER_NO, PRODUCTID, SUPPLIERID
                ORDER BY order_date DESC NULLS LAST
            ) AS rn
        FROM orders_parsed
    ),
    orders_latest AS (
        SELECT
            SALES_ORDER_NO,
            PRODUCTID,
            SUPPLIERID,
            CUSTOMERID,
            order_date,
            SALES_QUANTITY,
            ACCURATE_ORDER,
            return_flag,
            SHIPPING_COST,
            ON_TIME_DELIVERY,
            FINAL_PRICE,
            MODE_OF_ORDER,
            MODE_OF_SHIPPING,
            DISCOUNT_PER,
            SELLING_PRICE,
            expected_delivery_date,
            actual_delivery_date,
            ON_TIME_SHIPPING_FLAG,
            SALES_QUANTITY__BY_SELLING_PRICE
        FROM orders_dedup
        WHERE rn = 1
    ),
    inventory_parsed AS (
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
            COALESCE(
                TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR(INVENTORY_UPDATE_DATE), 'YYYY-MM-DD HH24:MI:SS'),
                TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR(INVENTORY_UPDATE_DATE), 'YYYY-MM-DD'),
                TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR(INVENTORY_UPDATE_DATE), 'DD-MM-YYYY'),
                TRY_TO_TIMESTAMP_NTZ(INVENTORY_UPDATE_DATE)
            ) AS inventory_update_ts
        FROM AI_ETL.AI_ETL_STG.STG_INVENTORY
    ),
    inventory_dedup AS (
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
            inventory_update_ts,
            ROW_NUMBER() OVER (
                PARTITION BY PRODUCT_ID, SUPPLIER_ID
                ORDER BY inventory_update_ts DESC NULLS LAST
            ) AS rn
        FROM inventory_parsed
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
            inventory_update_ts
        FROM inventory_dedup
        WHERE rn = 1
    ),
    product_clean AS (
        SELECT
            PRODUCT_ID,
            PRODUCT_NAME,
            PRODUCT_COMPONENT,
            UNIT_PRICE,
            COALESCE(
                TRY_TO_DATE(TO_VARCHAR(EOL_DATE), 'DD-MM-YYYY'),
                TRY_TO_DATE(TO_VARCHAR(EOL_DATE), 'YYYY-MM-DD'),
                TRY_TO_DATE(EOL_DATE)
            ) AS eol_date
        FROM AI_ETL.AI_ETL_STG.STG_PRODUCT
    ),
    supplier_clean AS (
        SELECT
            SUPPLIER_ID,
            SUPPLIER_NAME,
            SUPPLIER_CITY,
            STATE AS supplier_state,
            COUNTRY AS supplier_country,
            PHONE AS supplier_phone,
            EMAIL AS supplier_email,
            RISK AS supplier_risk,
            ROAD_TRANSPORT_COST,
            SEA_TRANSPORT_COST,
            AIR_TRANSPORT_COST
        FROM AI_ETL.AI_ETL_STG.STG_SUPPLIER
    ),
    customer_clean AS (
        SELECT
            CUSTOMERID,
            CUSTOMER_NAME,
            CUSTOMER_CITY,
            CUSTOMER_STATE,
            CUSTOMER_COUNTRY,
            CUSTOMER_COUNTRY_CODE,
            CUSTOMERSTATE_CODE,
            PHONE AS customer_phone,
            EMAIL AS customer_email
        FROM AI_ETL.AI_ETL_STG.STG_CUSTOMER
    ),
    final_base AS (
        SELECT
            o.SALES_ORDER_NO,
            o.PRODUCTID,
            o.SUPPLIERID,
            o.CUSTOMERID,
            o.order_date,
            o.expected_delivery_date,
            o.actual_delivery_date,
            o.SALES_QUANTITY,
            o.ACCURATE_ORDER,
            o.return_flag,
            o.SHIPPING_COST,
            o.ON_TIME_DELIVERY,
            o.FINAL_PRICE,
            o.MODE_OF_ORDER,
            o.MODE_OF_SHIPPING,
            o.DISCOUNT_PER,
            o.SELLING_PRICE,
            o.ON_TIME_SHIPPING_FLAG,
            o.SALES_QUANTITY__BY_SELLING_PRICE,
            c.CUSTOMER_NAME,
            c.CUSTOMER_CITY,
            c.CUSTOMER_STATE,
            c.CUSTOMER_COUNTRY,
            c.CUSTOMER_COUNTRY_CODE,
            c.CUSTOMERSTATE_CODE,
            c.customer_phone,
            c.customer_email,
            s.SUPPLIER_NAME,
            s.SUPPLIER_CITY,
            s.supplier_state,
            s.supplier_country,
            s.supplier_phone,
            s.supplier_email,
            s.supplier_risk,
            s.ROAD_TRANSPORT_COST,
            s.SEA_TRANSPORT_COST,
            s.AIR_TRANSPORT_COST,
            p.PRODUCT_NAME,
            p.PRODUCT_COMPONENT,
            p.UNIT_PRICE,
            p.eol_date,
            i.INVENTORY_UNITS,
            i.IN_OUT_STOCK,
            i.NO_OF_DAYS_OUT_OF_STOCK,
            i.INVENTORY_SERVICE_COSTS,
            i.INVENTORY_RISK_COSTS,
            i.STORAGE_COST,
            i.INVENTORY_CARRYING_COST,
            i.inventory_update_ts,
            CASE
                WHEN UPPER(o.MODE_OF_SHIPPING) = 'AIR' THEN COALESCE(s.AIR_TRANSPORT_COST, 0)
                WHEN UPPER(o.MODE_OF_SHIPPING) = 'SEA' THEN COALESCE(s.SEA_TRANSPORT_COST, 0)
                WHEN UPPER(o.MODE_OF_SHIPPING) = 'ROAD' THEN COALESCE(s.ROAD_TRANSPORT_COST, 0)
                ELSE COALESCE(s.ROAD_TRANSPORT_COST, COALESCE(s.SEA_TRANSPORT_COST, s.AIR_TRANSPORT_COST, 0))
            END AS shipping_factor_value
        FROM orders_latest o
        LEFT JOIN inventory_latest i
            ON o.PRODUCTID = i.PRODUCT_ID
           AND o.SUPPLIERID = i.SUPPLIER_ID
        LEFT JOIN product_clean p
            ON o.PRODUCTID = p.PRODUCT_ID
        LEFT JOIN supplier_clean s
            ON o.SUPPLIERID = s.SUPPLIER_ID
        LEFT JOIN customer_clean c
            ON o.CUSTOMERID = c.CUSTOMERID
    ),
    final_enriched AS (
        SELECT
            final_base.SALES_ORDER_NO,
            final_base.PRODUCTID,
            final_base.SUPPLIERID,
            final_base.CUSTOMERID,
            final_base.order_date,
            final_base.expected_delivery_date,
            final_base.actual_delivery_date,
            final_base.SALES_QUANTITY,
            final_base.ACCURATE_ORDER,
            final_base.return_flag,
            final_base.SHIPPING_COST,
            final_base.ON_TIME_DELIVERY,
            final_base.FINAL_PRICE,
            final_base.MODE_OF_ORDER,
            final_base.MODE_OF_SHIPPING,
            final_base.DISCOUNT_PER,
            final_base.SELLING_PRICE,
            final_base.ON_TIME_SHIPPING_FLAG,
            final_base.SALES_QUANTITY__BY_SELLING_PRICE,
            final_base.CUSTOMER_NAME,
            final_base.CUSTOMER_CITY,
            final_base.CUSTOMER_STATE,
            final_base.CUSTOMER_COUNTRY,
            final_base.CUSTOMER_COUNTRY_CODE,
            final_base.CUSTOMERSTATE_CODE,
            final_base.customer_phone,
            final_base.customer_email,
            final_base.SUPPLIER_NAME,
            final_base.SUPPLIER_CITY,
            final_base.supplier_state,
            final_base.supplier_country,
            final_base.supplier_phone,
            final_base.supplier_email,
            final_base.supplier_risk,
            final_base.ROAD_TRANSPORT_COST,
            final_base.SEA_TRANSPORT_COST,
            final_base.AIR_TRANSPORT_COST,
            final_base.PRODUCT_NAME,
            final_base.PRODUCT_COMPONENT,
            final_base.UNIT_PRICE,
            final_base.eol_date,
            final_base.INVENTORY_UNITS,
            final_base.IN_OUT_STOCK,
            final_base.NO_OF_DAYS_OUT_OF_STOCK,
            final_base.INVENTORY_SERVICE_COSTS,
            final_base.INVENTORY_RISK_COSTS,
            final_base.STORAGE_COST,
            final_base.INVENTORY_CARRYING_COST,
            final_base.inventory_update_ts,
            final_base.shipping_factor_value AS shipping_factor,
            CASE WHEN final_base.return_flag = TRUE THEN 1 ELSE 0 END AS return_indicator,
            CASE WHEN final_base.ON_TIME_DELIVERY = TRUE AND final_base.ACCURATE_ORDER = TRUE THEN 1 ELSE 0 END AS otif_flag
        FROM final_base
    ),
    final_metrics AS (
        SELECT
            final_enriched.SALES_ORDER_NO,
            final_enriched.PRODUCTID,
            final_enriched.SUPPLIERID,
            final_enriched.CUSTOMERID,
            final_enriched.order_date,
            final_enriched.expected_delivery_date,
            final_enriched.actual_delivery_date,
            final_enriched.SALES_QUANTITY,
            final_enriched.ACCURATE_ORDER,
            final_enriched.return_flag,
            final_enriched.return_indicator,
            final_enriched.SHIPPING_COST,
            final_enriched.ON_TIME_DELIVERY,
            final_enriched.FINAL_PRICE,
            final_enriched.MODE_OF_ORDER,
            final_enriched.MODE_OF_SHIPPING,
            final_enriched.DISCOUNT_PER,
            final_enriched.SELLING_PRICE,
            final_enriched.ON_TIME_SHIPPING_FLAG,
            final_enriched.SALES_QUANTITY__BY_SELLING_PRICE,
            final_enriched.CUSTOMER_NAME,
            final_enriched.CUSTOMER_CITY,
            final_enriched.CUSTOMER_STATE,
            final_enriched.CUSTOMER_COUNTRY,
            final_enriched.CUSTOMER_COUNTRY_CODE,
            final_enriched.CUSTOMERSTATE_CODE,
            final_enriched.customer_phone,
            final_enriched.customer_email,
            final_enriched.SUPPLIER_NAME,
            final_enriched.SUPPLIER_CITY,
            final_enriched.supplier_state,
            final_enriched.supplier_country,
            final_enriched.supplier_phone,
            final_enriched.supplier_email,
            final_enriched.supplier_risk,
            final_enriched.ROAD_TRANSPORT_COST,
            final_enriched.SEA_TRANSPORT_COST,
            final_enriched.AIR_TRANSPORT_COST,
            final_enriched.PRODUCT_NAME,
            final_enriched.PRODUCT_COMPONENT,
            final_enriched.UNIT_PRICE,
            final_enriched.eol_date,
            final_enriched.INVENTORY_UNITS,
            final_enriched.IN_OUT_STOCK,
            final_enriched.NO_OF_DAYS_OUT_OF_STOCK,
            final_enriched.INVENTORY_SERVICE_COSTS,
            final_enriched.INVENTORY_RISK_COSTS,
            final_enriched.STORAGE_COST,
            final_enriched.INVENTORY_CARRYING_COST,
            final_enriched.inventory_update_ts,
            final_enriched.shipping_factor,
            final_enriched.otif_flag,
            CASE
                WHEN final_enriched.sales_quantity IS NULL OR final_enriched.sales_quantity = 0 THEN NULL
                ELSE final_enriched.INVENTORY_UNITS / final_enriched.sales_quantity
            END AS dsi_estimate,
            COALESCE(final_enriched.UNIT_PRICE, 0) + COALESCE(final_enriched.SHIPPING_COST, 0) + COALESCE(final_enriched.INVENTORY_SERVICE_COSTS, 0) + COALESCE(final_enriched.STORAGE_COST, 0) AS landed_cost_per_unit,
            CASE
                WHEN final_enriched.UNIT_PRICE IS NULL OR final_enriched.INVENTORY_UNITS IS NULL OR final_enriched.UNIT_PRICE = 0 OR final_enriched.INVENTORY_UNITS = 0 THEN NULL
                ELSE ((COALESCE(final_enriched.FINAL_PRICE, 0) - COALESCE(final_enriched.UNIT_PRICE, 0)) * COALESCE(final_enriched.SALES_QUANTITY, 0)) / (final_enriched.UNIT_PRICE * final_enriched.INVENTORY_UNITS)
            END AS gmroi,
            DATEDIFF('day', final_enriched.order_date, final_enriched.actual_delivery_date) AS order_to_cash_cycle_days,
            CASE
                WHEN final_enriched.sales_quantity IS NULL OR final_enriched.sales_quantity = 0 THEN NULL
                ELSE final_enriched.return_indicator / final_enriched.sales_quantity
            END AS return_to_sales_ratio,
            CASE
                WHEN final_enriched.sales_quantity IS NULL OR final_enriched.sales_quantity = 0 THEN NULL
                ELSE COALESCE(final_enriched.SHIPPING_COST, 0) / final_enriched.sales_quantity
            END AS last_mile_efficiency,
            CASE
                WHEN final_enriched.sales_quantity IS NULL OR final_enriched.sales_quantity = 0 THEN NULL
                ELSE COALESCE(final_enriched.NO_OF_DAYS_OUT_OF_STOCK, 0) / final_enriched.sales_quantity
            END AS stockout_risk_score,
            COALESCE(final_enriched.SALES_QUANTITY, 0) * COALESCE(final_enriched.shipping_factor, 0) AS carbon_footprint_est,
            DATEDIFF('day', final_enriched.expected_delivery_date, final_enriched.actual_delivery_date) AS carrier_lead_time_variance
        FROM final_enriched
    ),
    final_dedup AS (
        SELECT
            final_metrics.*,
            ROW_NUMBER() OVER (
                PARTITION BY final_metrics.SALES_ORDER_NO, final_metrics.PRODUCTID, final_metrics.SUPPLIERID
                ORDER BY final_metrics.order_date DESC NULLS LAST
            ) AS rn
        FROM final_metrics
    )
    SELECT
        final_dedup.SALES_ORDER_NO AS SALES_ORDER_NO,
        final_dedup.PRODUCTID AS PRODUCTID,
        final_dedup.SUPPLIERID AS SUPPLIERID,
        final_dedup.CUSTOMERID AS CUSTOMERID,
        final_dedup.order_date AS ORDER_DATE,
        final_dedup.expected_delivery_date AS EXPECTED_DELIVERY_DATE,
        final_dedup.actual_delivery_date AS ACTUAL_DELIVERY_DATE,
        final_dedup.SALES_QUANTITY AS SALES_QUANTITY,
        final_dedup.ACCURATE_ORDER AS ACCURATE_ORDER,
        final_dedup.return_flag AS RETURN_FLAG,
        final_dedup.return_indicator AS RETURN_INDICATOR,
        final_dedup.SHIPPING_COST AS SHIPPING_COST,
        final_dedup.ON_TIME_DELIVERY AS ON_TIME_DELIVERY,
        final_dedup.FINAL_PRICE AS FINAL_PRICE,
        final_dedup.MODE_OF_ORDER AS MODE_OF_ORDER,
        final_dedup.MODE_OF_SHIPPING AS MODE_OF_SHIPPING,
        final_dedup.DISCOUNT_PER AS DISCOUNT_PER,
        final_dedup.SELLING_PRICE AS SELLING_PRICE,
        final_dedup.ON_TIME_SHIPPING_FLAG AS ON_TIME_SHIPPING_FLAG,
        final_dedup.SALES_QUANTITY__BY_SELLING_PRICE AS SALES_QUANTITY__BY_SELLING_PRICE,
        final_dedup.CUSTOMER_NAME AS CUSTOMER_NAME,
        final_dedup.CUSTOMER_CITY AS CUSTOMER_CITY,
        final_dedup.CUSTOMER_STATE AS CUSTOMER_STATE,
        final_dedup.CUSTOMER_COUNTRY AS CUSTOMER_COUNTRY,
        final_dedup.CUSTOMER_COUNTRY_CODE AS CUSTOMER_COUNTRY_CODE,
        final_dedup.CUSTOMERSTATE_CODE AS CUSTOMERSTATE_CODE,
        final_dedup.customer_phone AS CUSTOMER_PHONE,
        final_dedup.customer_email AS CUSTOMER_EMAIL,
        final_dedup.SUPPLIER_NAME AS SUPPLIER_NAME,
        final_dedup.SUPPLIER_CITY AS SUPPLIER_CITY,
        final_dedup.supplier_state AS SUPPLIER_STATE,
        final_dedup.supplier_country AS SUPPLIER_COUNTRY,
        final_dedup.supplier_phone AS SUPPLIER_PHONE,
        final_dedup.supplier_email AS SUPPLIER_EMAIL,
        final_dedup.supplier_risk AS SUPPLIER_RISK,
        final_dedup.ROAD_TRANSPORT_COST AS ROAD_TRANSPORT_COST,
        final_dedup.SEA_TRANSPORT_COST AS SEA_TRANSPORT_COST,
        final_dedup.AIR_TRANSPORT_COST AS AIR_TRANSPORT_COST,
        final_dedup.PRODUCT_NAME AS PRODUCT_NAME,
        final_dedup.PRODUCT_COMPONENT AS PRODUCT_COMPONENT,
        final_dedup.UNIT_PRICE AS UNIT_PRICE,
        final_dedup.eol_date AS EOL_DATE,
        final_dedup.INVENTORY_UNITS AS INVENTORY_UNITS,
        final_dedup.IN_OUT_STOCK AS IN_OUT_STOCK,
        final_dedup.NO_OF_DAYS_OUT_OF_STOCK AS NO_OF_DAYS_OUT_OF_STOCK,
        final_dedup.INVENTORY_SERVICE_COSTS AS INVENTORY_SERVICE_COSTS,
        final_dedup.INVENTORY_RISK_COSTS AS INVENTORY_RISK_COSTS,
        final_dedup.STORAGE_COST AS STORAGE_COST,
        final_dedup.INVENTORY_CARRYING_COST AS INVENTORY_CARRYING_COST,
        final_dedup.inventory_update_ts AS INVENTORY_UPDATE_TS,
        final_dedup.shipping_factor AS SHIPPING_FACTOR,
        final_dedup.otif_flag AS OTIF_FLAG,
        final_dedup.landed_cost_per_unit AS LANDED_COST_PER_UNIT,
        final_dedup.dsi_estimate AS DSI_ESTIMATE,
        final_dedup.gmroi AS GMROI,
        final_dedup.order_to_cash_cycle_days AS ORDER_TO_CASH_CYCLE_DAYS,
        final_dedup.return_to_sales_ratio AS RETURN_TO_SALES_RATIO,
        final_dedup.last_mile_efficiency AS LAST_MILE_EFFICIENCY,
        final_dedup.stockout_risk_score AS STOCKOUT_RISK_SCORE,
        final_dedup.carbon_footprint_est AS CARBON_FOOTPRINT_EST,
        final_dedup.carrier_lead_time_variance AS CARRIER_LEAD_TIME_VARIANCE
    FROM final_dedup
    WHERE rn = 1
) AS S
ON (
    T.SALES_ORDER_NO = S.SALES_ORDER_NO
    AND T.PRODUCTID = S.PRODUCTID
    AND T.SUPPLIERID = S.SUPPLIERID
)
WHEN MATCHED THEN UPDATE SET
    T.SALES_ORDER_NO = S.SALES_ORDER_NO,
    T.PRODUCTID = S.PRODUCTID,
    T.SUPPLIERID = S.SUPPLIERID,
    T.CUSTOMERID = S.CUSTOMERID,
    T.ORDER_DATE = S.ORDER_DATE,
    T.EXPECTED_DELIVERY_DATE = S.EXPECTED_DELIVERY_DATE,
    T.ACTUAL_DELIVERY_DATE = S.ACTUAL_DELIVERY_DATE,
    T.SALES_QUANTITY = S.SALES_QUANTITY,
    T.ACCURATE_ORDER = S.ACCURATE_ORDER,
    T.RETURN_FLAG = S.RETURN_FLAG,
    T.RETURN_INDICATOR = S.RETURN_INDICATOR,
    T.SHIPPING_COST = S.SHIPPING_COST,
    T.ON_TIME_DELIVERY = S.ON_TIME_DELIVERY,
    T.FINAL_PRICE = S.FINAL_PRICE,
    T.MODE_OF_ORDER = S.MODE_OF_ORDER,
    T.MODE_OF_SHIPPING = S.MODE_OF_SHIPPING,
    T.DISCOUNT_PER = S.DISCOUNT_PER,
    T.SELLING_PRICE = S.SELLING_PRICE,
    T.ON_TIME_SHIPPING_FLAG = S.ON_TIME_SHIPPING_FLAG,
    T.SALES_QUANTITY__BY_SELLING_PRICE = S.SALES_QUANTITY__BY_SELLING_PRICE,
    T.CUSTOMER_NAME = S.CUSTOMER_NAME,
    T.CUSTOMER_CITY = S.CUSTOMER_CITY,
    T.CUSTOMER_STATE = S.CUSTOMER_STATE,
    T.CUSTOMER_COUNTRY = S.CUSTOMER_COUNTRY,
    T.CUSTOMER_COUNTRY_CODE = S.CUSTOMER_COUNTRY_CODE,
    T.CUSTOMERSTATE_CODE = S.CUSTOMERSTATE_CODE,
    T.CUSTOMER_PHONE = S.CUSTOMER_PHONE,
    T.CUSTOMER_EMAIL = S.CUSTOMER_EMAIL,
    T.SUPPLIER_NAME = S.SUPPLIER_NAME,
    T.SUPPLIER_CITY = S.SUPPLIER_CITY,
    T.SUPPLIER_STATE = S.SUPPLIER_STATE,
    T.SUPPLIER_COUNTRY = S.SUPPLIER_COUNTRY,
    T.SUPPLIER_PHONE = S.SUPPLIER_PHONE,
    T.SUPPLIER_EMAIL = S.SUPPLIER_EMAIL,
    T.SUPPLIER_RISK = S.SUPPLIER_RISK,
    T.ROAD_TRANSPORT_COST = S.ROAD_TRANSPORT_COST,
    T.SEA_TRANSPORT_COST = S.SEA_TRANSPORT_COST,
    T.AIR_TRANSPORT_COST = S.AIR_TRANSPORT_COST,
    T.PRODUCT_NAME = S.PRODUCT_NAME,
    T.PRODUCT_COMPONENT = S.PRODUCT_COMPONENT,
    T.UNIT_PRICE = S.UNIT_PRICE,
    T.EOL_DATE = S.EOL_DATE,
    T.INVENTORY_UNITS = S.INVENTORY_UNITS,
    T.IN_OUT_STOCK = S.IN_OUT_STOCK,
    T.NO_OF_DAYS_OUT_OF_STOCK = S.NO_OF_DAYS_OUT_OF_STOCK,
    T.INVENTORY_SERVICE_COSTS = S.INVENTORY_SERVICE_COSTS,
    T.INVENTORY_RISK_COSTS = S.INVENTORY_RISK_COSTS,
    T.STORAGE_COST = S.STORAGE_COST,
    T.INVENTORY_CARRYING_COST = S.INVENTORY_CARRYING_COST,
    T.INVENTORY_UPDATE_TS = S.INVENTORY_UPDATE_TS,
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
    ORDER_DATE,
    EXPECTED_DELIVERY_DATE,
    ACTUAL_DELIVERY_DATE,
    SALES_QUANTITY,
    ACCURATE_ORDER,
    RETURN_FLAG,
    RETURN_INDICATOR,
    SHIPPING_COST,
    ON_TIME_DELIVERY,
    FINAL_PRICE,
    MODE_OF_ORDER,
    MODE_OF_SHIPPING,
    DISCOUNT_PER,
    SELLING_PRICE,
    ON_TIME_SHIPPING_FLAG,
    SALES_QUANTITY__BY_SELLING_PRICE,
    CUSTOMER_NAME,
    CUSTOMER_CITY,
    CUSTOMER_STATE,
    CUSTOMER_COUNTRY,
    CUSTOMER_COUNTRY_CODE,
    CUSTOMERSTATE_CODE,
    CUSTOMER_PHONE,
    CUSTOMER_EMAIL,
    SUPPLIER_NAME,
    SUPPLIER_CITY,
    SUPPLIER_STATE,
    SUPPLIER_COUNTRY,
    SUPPLIER_PHONE,
    SUPPLIER_EMAIL,
    SUPPLIER_RISK,
    ROAD_TRANSPORT_COST,
    SEA_TRANSPORT_COST,
    AIR_TRANSPORT_COST,
    PRODUCT_NAME,
    PRODUCT_COMPONENT,
    UNIT_PRICE,
    EOL_DATE,
    INVENTORY_UNITS,
    IN_OUT_STOCK,
    NO_OF_DAYS_OUT_OF_STOCK,
    INVENTORY_SERVICE_COSTS,
    INVENTORY_RISK_COSTS,
    STORAGE_COST,
    INVENTORY_CARRYING_COST,
    INVENTORY_UPDATE_TS,
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
) VALUES (
    S.SALES_ORDER_NO,
    S.PRODUCTID,
    S.SUPPLIERID,
    S.CUSTOMERID,
    S.ORDER_DATE,
    S.EXPECTED_DELIVERY_DATE,
    S.ACTUAL_DELIVERY_DATE,
    S.SALES_QUANTITY,
    S.ACCURATE_ORDER,
    S.RETURN_FLAG,
    S.RETURN_INDICATOR,
    S.SHIPPING_COST,
    S.ON_TIME_DELIVERY,
    S.FINAL_PRICE,
    S.MODE_OF_ORDER,
    S.MODE_OF_SHIPPING,
    S.DISCOUNT_PER,
    S.SELLING_PRICE,
    S.ON_TIME_SHIPPING_FLAG,
    S.SALES_QUANTITY__BY_SELLING_PRICE,
    S.CUSTOMER_NAME,
    S.CUSTOMER_CITY,
    S.CUSTOMER_STATE,
    S.CUSTOMER_COUNTRY,
    S.CUSTOMER_COUNTRY_CODE,
    S.CUSTOMERSTATE_CODE,
    S.CUSTOMER_PHONE,
    S.CUSTOMER_EMAIL,
    S.SUPPLIER_NAME,
    S.SUPPLIER_CITY,
    S.SUPPLIER_STATE,
    S.SUPPLIER_COUNTRY,
    S.SUPPLIER_PHONE,
    S.SUPPLIER_EMAIL,
    S.SUPPLIER_RISK,
    S.ROAD_TRANSPORT_COST,
    S.SEA_TRANSPORT_COST,
    S.AIR_TRANSPORT_COST,
    S.PRODUCT_NAME,
    S.PRODUCT_COMPONENT,
    S.UNIT_PRICE,
    S.EOL_DATE,
    S.INVENTORY_UNITS,
    S.IN_OUT_STOCK,
    S.NO_OF_DAYS_OUT_OF_STOCK,
    S.INVENTORY_SERVICE_COSTS,
    S.INVENTORY_RISK_COSTS,
    S.STORAGE_COST,
    S.INVENTORY_CARRYING_COST,
    S.INVENTORY_UPDATE_TS,
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