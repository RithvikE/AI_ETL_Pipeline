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
        ) AS ORDER_DATE,
        COALESCE(
            TRY_TO_DATE(TO_VARCHAR(EXPECTED_DELIVERY_DATE), 'DD-MM-YYYY'),
            TRY_TO_DATE(TO_VARCHAR(EXPECTED_DELIVERY_DATE), 'YYYY-MM-DD'),
            TRY_TO_DATE(EXPECTED_DELIVERY_DATE)
        ) AS EXPECTED_DELIVERY_DATE,
        COALESCE(
            TRY_TO_DATE(TO_VARCHAR(ACTUAL_DELIVERY_DATE), 'DD-MM-YYYY'),
            TRY_TO_DATE(TO_VARCHAR(ACTUAL_DELIVERY_DATE), 'YYYY-MM-DD'),
            TRY_TO_DATE(ACTUAL_DELIVERY_DATE)
        ) AS ACTUAL_DELIVERY_DATE,
        SALES_QUANTITY,
        ACCURATE_ORDER,
        "RETURN" AS ORDER_RETURN_FLAG,
        CASE WHEN "RETURN" THEN 1 ELSE 0 END AS RETURN_QUANTITY,
        DEFECTIVE_UNIT,
        DEFECT_COUNT,
        MODE_OF_ORDER,
        SHIPPING_COST,
        ON_TIME_SHIPPING_FLAG,
        ON_TIME_DELIVERY,
        SELLING_PRICE,
        DISCOUNT_PER,
        FINAL_PRICE,
        MODE_OF_SHIPPING
    FROM AI_ETL.AI_ETL_STG.STG_ORDERS
),
orders_dedup AS (
    SELECT
        orders_parsed.*,
        ROW_NUMBER() OVER (
            PARTITION BY orders_parsed.SALES_ORDER_NO, orders_parsed.PRODUCTID, orders_parsed.SUPPLIERID
            ORDER BY orders_parsed.ORDER_DATE DESC, orders_parsed.ACTUAL_DELIVERY_DATE DESC
        ) AS rn
    FROM orders_parsed
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
            TRY_TO_TIMESTAMP(TO_VARCHAR(INVENTORY_UPDATE_DATE), 'DD-MM-YYYY HH24:MI:SS'),
            TRY_TO_TIMESTAMP(TO_VARCHAR(INVENTORY_UPDATE_DATE), 'YYYY-MM-DD HH24:MI:SS'),
            TRY_TO_TIMESTAMP(TO_VARCHAR(INVENTORY_UPDATE_DATE), 'YYYY-MM-DD'),
            TRY_TO_TIMESTAMP(INVENTORY_UPDATE_DATE)
        ) AS INVENTORY_UPDATE_TS
    FROM AI_ETL.AI_ETL_STG.STG_INVENTORY
),
inventory_dedup AS (
    SELECT
        inventory_parsed.*,
        ROW_NUMBER() OVER (
            PARTITION BY inventory_parsed.PRODUCT_ID, inventory_parsed.SUPPLIER_ID
            ORDER BY inventory_parsed.INVENTORY_UPDATE_TS DESC
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
        INVENTORY_UPDATE_TS
    FROM inventory_dedup
    WHERE rn = 1
),
product_enriched AS (
    SELECT
        PRODUCT_ID,
        PRODUCT_NAME,
        PRODUCT_COMPONENT,
        UNIT_PRICE,
        COALESCE(
            TRY_TO_DATE(TO_VARCHAR(EOL_DATE), 'DD-MM-YYYY'),
            TRY_TO_DATE(TO_VARCHAR(EOL_DATE), 'YYYY-MM-DD'),
            TRY_TO_DATE(EOL_DATE)
        ) AS PRODUCT_EOL_DATE,
        ROW_NUMBER() OVER (
            PARTITION BY PRODUCT_ID
            ORDER BY PRODUCT_ID
        ) AS rn
    FROM AI_ETL.AI_ETL_STG.STG_PRODUCT
),
product_latest AS (
    SELECT
        PRODUCT_ID,
        PRODUCT_NAME,
        PRODUCT_COMPONENT,
        UNIT_PRICE,
        PRODUCT_EOL_DATE
    FROM product_enriched
    WHERE rn = 1
),
supplier_enriched AS (
    SELECT
        SUPPLIER_ID,
        SUPPLIER_NAME,
        SUPPLIER_CITY,
        STATE AS SUPPLIER_STATE,
        COUNTRY AS SUPPLIER_COUNTRY,
        ROAD_TRANSPORT_COST,
        SEA_TRANSPORT_COST,
        AIR_TRANSPORT_COST,
        ROW_NUMBER() OVER (
            PARTITION BY SUPPLIER_ID
            ORDER BY SUPPLIER_ID
        ) AS rn
    FROM AI_ETL.AI_ETL_STG.STG_SUPPLIER
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
    FROM supplier_enriched
    WHERE rn = 1
),
customer_enriched AS (
    SELECT
        CUSTOMERID,
        CUSTOMER_NAME,
        CUSTOMER_CITY,
        CUSTOMER_STATE,
        CUSTOMER_COUNTRY,
        ROW_NUMBER() OVER (
            PARTITION BY CUSTOMERID
            ORDER BY CUSTOMERID
        ) AS rn
    FROM AI_ETL.AI_ETL_STG.STG_CUSTOMER
),
customer_latest AS (
    SELECT
        CUSTOMERID,
        CUSTOMER_NAME,
        CUSTOMER_CITY,
        CUSTOMER_STATE,
        CUSTOMER_COUNTRY
    FROM customer_enriched
    WHERE rn = 1
),
joined_data AS (
    SELECT
        ord.SALES_ORDER_NO,
        ord.PRODUCTID,
        ord.SUPPLIERID,
        ord.CUSTOMERID,
        cust.CUSTOMER_NAME,
        cust.CUSTOMER_CITY,
        cust.CUSTOMER_STATE,
        cust.CUSTOMER_COUNTRY,
        prod.PRODUCT_NAME,
        prod.PRODUCT_COMPONENT,
        prod.UNIT_PRICE,
        prod.PRODUCT_EOL_DATE,
        sup.SUPPLIER_NAME,
        sup.SUPPLIER_CITY,
        sup.SUPPLIER_STATE,
        sup.SUPPLIER_COUNTRY,
        sup.ROAD_TRANSPORT_COST,
        sup.SEA_TRANSPORT_COST,
        sup.AIR_TRANSPORT_COST,
        ord.ORDER_DATE,
        ord.EXPECTED_DELIVERY_DATE,
        ord.ACTUAL_DELIVERY_DATE,
        ord.SALES_QUANTITY,
        ord.ACCURATE_ORDER,
        ord.ORDER_RETURN_FLAG,
        ord.RETURN_QUANTITY,
        ord.DEFECTIVE_UNIT,
        ord.DEFECT_COUNT,
        ord.MODE_OF_ORDER,
        ord.SHIPPING_COST,
        ord.ON_TIME_SHIPPING_FLAG,
        ord.ON_TIME_DELIVERY,
        ord.SELLING_PRICE,
        ord.DISCOUNT_PER,
        ord.FINAL_PRICE,
        ord.MODE_OF_SHIPPING,
        inv.INVENTORY_UNITS,
        inv.IN_OUT_STOCK,
        inv.NO_OF_DAYS_OUT_OF_STOCK,
        inv.INVENTORY_SERVICE_COSTS,
        inv.INVENTORY_RISK_COSTS,
        inv.STORAGE_COST,
        inv.INVENTORY_CARRYING_COST,
        inv.INVENTORY_UPDATE_TS
    FROM orders_dedup ord
    LEFT JOIN inventory_latest inv
        ON ord.PRODUCTID = inv.PRODUCT_ID
        AND ord.SUPPLIERID = inv.SUPPLIER_ID
    LEFT JOIN product_latest prod
        ON ord.PRODUCTID = prod.PRODUCT_ID
    LEFT JOIN supplier_latest sup
        ON ord.SUPPLIERID = sup.SUPPLIER_ID
    LEFT JOIN customer_latest cust
        ON ord.CUSTOMERID = cust.CUSTOMERID
    WHERE ord.rn = 1
),
final_base AS (
    SELECT
        joined_data.SALES_ORDER_NO,
        joined_data.PRODUCTID,
        joined_data.SUPPLIERID,
        joined_data.CUSTOMERID,
        joined_data.CUSTOMER_NAME,
        joined_data.CUSTOMER_CITY,
        joined_data.CUSTOMER_STATE,
        joined_data.CUSTOMER_COUNTRY,
        joined_data.PRODUCT_NAME,
        joined_data.PRODUCT_COMPONENT,
        joined_data.PRODUCT_EOL_DATE,
        joined_data.SUPPLIER_NAME,
        joined_data.SUPPLIER_CITY,
        joined_data.SUPPLIER_STATE,
        joined_data.SUPPLIER_COUNTRY,
        joined_data.ROAD_TRANSPORT_COST,
        joined_data.SEA_TRANSPORT_COST,
        joined_data.AIR_TRANSPORT_COST,
        joined_data.ORDER_DATE,
        joined_data.EXPECTED_DELIVERY_DATE,
        joined_data.ACTUAL_DELIVERY_DATE,
        joined_data.SALES_QUANTITY,
        joined_data.UNIT_PRICE,
        joined_data.SELLING_PRICE,
        joined_data.DISCOUNT_PER,
        joined_data.FINAL_PRICE,
        joined_data.MODE_OF_ORDER,
        joined_data.MODE_OF_SHIPPING,
        joined_data.SHIPPING_COST,
        joined_data.ON_TIME_SHIPPING_FLAG,
        joined_data.ON_TIME_DELIVERY,
        joined_data.ACCURATE_ORDER,
        joined_data.ORDER_RETURN_FLAG,
        joined_data.RETURN_QUANTITY,
        joined_data.DEFECTIVE_UNIT,
        joined_data.DEFECT_COUNT,
        joined_data.INVENTORY_UNITS,
        joined_data.IN_OUT_STOCK,
        joined_data.NO_OF_DAYS_OUT_OF_STOCK,
        joined_data.INVENTORY_SERVICE_COSTS,
        joined_data.INVENTORY_RISK_COSTS,
        joined_data.STORAGE_COST,
        joined_data.INVENTORY_CARRYING_COST,
        joined_data.INVENTORY_UPDATE_TS,
        CASE
            WHEN joined_data.MODE_OF_SHIPPING IS NULL THEN 1
            WHEN UPPER(joined_data.MODE_OF_SHIPPING) IN ('AIR', 'AIR FREIGHT') THEN 5
            WHEN UPPER(joined_data.MODE_OF_SHIPPING) IN ('SEA', 'SEA FREIGHT', 'OCEAN') THEN 2
            WHEN UPPER(joined_data.MODE_OF_SHIPPING) IN ('ROAD', 'GROUND', 'TRUCK') THEN 1.5
            WHEN UPPER(joined_data.MODE_OF_SHIPPING) = 'RAIL' THEN 1.2
            ELSE 1
        END AS SHIPPING_FACTOR
    FROM joined_data
),
final_metrics AS (
    SELECT
        final_base.SALES_ORDER_NO,
        final_base.PRODUCTID,
        final_base.SUPPLIERID,
        final_base.CUSTOMERID,
        final_base.CUSTOMER_NAME,
        final_base.CUSTOMER_CITY,
        final_base.CUSTOMER_STATE,
        final_base.CUSTOMER_COUNTRY,
        final_base.PRODUCT_NAME,
        final_base.PRODUCT_COMPONENT,
        final_base.PRODUCT_EOL_DATE,
        final_base.SUPPLIER_NAME,
        final_base.SUPPLIER_CITY,
        final_base.SUPPLIER_STATE,
        final_base.SUPPLIER_COUNTRY,
        final_base.ROAD_TRANSPORT_COST,
        final_base.SEA_TRANSPORT_COST,
        final_base.AIR_TRANSPORT_COST,
        final_base.ORDER_DATE,
        final_base.EXPECTED_DELIVERY_DATE,
        final_base.ACTUAL_DELIVERY_DATE,
        final_base.SALES_QUANTITY,
        final_base.UNIT_PRICE,
        final_base.SELLING_PRICE,
        final_base.DISCOUNT_PER,
        final_base.FINAL_PRICE,
        final_base.MODE_OF_ORDER,
        final_base.MODE_OF_SHIPPING,
        final_base.SHIPPING_COST,
        final_base.SHIPPING_FACTOR,
        final_base.ON_TIME_SHIPPING_FLAG,
        final_base.ON_TIME_DELIVERY,
        final_base.ACCURATE_ORDER,
        final_base.ORDER_RETURN_FLAG,
        final_base.RETURN_QUANTITY,
        final_base.DEFECTIVE_UNIT,
        final_base.DEFECT_COUNT,
        final_base.INVENTORY_UNITS,
        final_base.IN_OUT_STOCK,
        final_base.NO_OF_DAYS_OUT_OF_STOCK,
        final_base.INVENTORY_SERVICE_COSTS,
        final_base.INVENTORY_RISK_COSTS,
        final_base.STORAGE_COST,
        final_base.INVENTORY_CARRYING_COST,
        final_base.INVENTORY_UPDATE_TS,
        CASE WHEN final_base.ON_TIME_DELIVERY = TRUE AND final_base.ACCURATE_ORDER = TRUE THEN 1 ELSE 0 END AS OTIF_FLAG,
        COALESCE(final_base.UNIT_PRICE, 0) + COALESCE(final_base.SHIPPING_COST, 0) + COALESCE(final_base.INVENTORY_SERVICE_COSTS, 0) + COALESCE(final_base.STORAGE_COST, 0) AS LANDED_COST_PER_UNIT,
        final_base.INVENTORY_UNITS / NULLIF(final_base.SALES_QUANTITY, 0) AS DSI_ESTIMATE,
        ((COALESCE(final_base.FINAL_PRICE, 0) - COALESCE(final_base.UNIT_PRICE, 0)) * COALESCE(final_base.SALES_QUANTITY, 0)) / NULLIF(COALESCE(final_base.UNIT_PRICE, 0) * COALESCE(final_base.INVENTORY_UNITS, 0), 0) AS GMROI,
        DATEDIFF('day', final_base.ORDER_DATE, final_base.ACTUAL_DELIVERY_DATE) AS ORDER_TO_CASH_CYCLE_DAYS,
        final_base.RETURN_QUANTITY / NULLIF(final_base.SALES_QUANTITY, 0) AS RETURN_TO_SALES_RATIO,
        COALESCE(final_base.SHIPPING_COST, 0) / NULLIF(final_base.SALES_QUANTITY, 0) AS LAST_MILE_EFFICIENCY,
        final_base.NO_OF_DAYS_OUT_OF_STOCK / NULLIF(final_base.SALES_QUANTITY, 0) AS STOCKOUT_RISK_SCORE,
        COALESCE(final_base.SALES_QUANTITY, 0) * final_base.SHIPPING_FACTOR AS CARBON_FOOTPRINT_EST,
        DATEDIFF('day', final_base.EXPECTED_DELIVERY_DATE, final_base.ACTUAL_DELIVERY_DATE) AS CARRIER_LEAD_TIME_VARIANCE
    FROM final_base
),
final_dataset AS (
    SELECT
        ranked.SALES_ORDER_NO,
        ranked.PRODUCTID,
        ranked.SUPPLIERID,
        ranked.CUSTOMERID,
        ranked.CUSTOMER_NAME,
        ranked.CUSTOMER_CITY,
        ranked.CUSTOMER_STATE,
        ranked.CUSTOMER_COUNTRY,
        ranked.PRODUCT_NAME,
        ranked.PRODUCT_COMPONENT,
        ranked.PRODUCT_EOL_DATE,
        ranked.SUPPLIER_NAME,
        ranked.SUPPLIER_CITY,
        ranked.SUPPLIER_STATE,
        ranked.SUPPLIER_COUNTRY,
        ranked.ROAD_TRANSPORT_COST,
        ranked.SEA_TRANSPORT_COST,
        ranked.AIR_TRANSPORT_COST,
        ranked.ORDER_DATE,
        ranked.EXPECTED_DELIVERY_DATE,
        ranked.ACTUAL_DELIVERY_DATE,
        ranked.SALES_QUANTITY,
        ranked.UNIT_PRICE,
        ranked.SELLING_PRICE,
        ranked.DISCOUNT_PER,
        ranked.FINAL_PRICE,
        ranked.MODE_OF_ORDER,
        ranked.MODE_OF_SHIPPING,
        ranked.SHIPPING_COST,
        ranked.SHIPPING_FACTOR,
        ranked.ON_TIME_SHIPPING_FLAG,
        ranked.ON_TIME_DELIVERY,
        ranked.ACCURATE_ORDER,
        ranked.ORDER_RETURN_FLAG,
        ranked.RETURN_QUANTITY,
        ranked.DEFECTIVE_UNIT,
        ranked.DEFECT_COUNT,
        ranked.INVENTORY_UNITS,
        ranked.IN_OUT_STOCK,
        ranked.NO_OF_DAYS_OUT_OF_STOCK,
        ranked.INVENTORY_SERVICE_COSTS,
        ranked.INVENTORY_RISK_COSTS,
        ranked.STORAGE_COST,
        ranked.INVENTORY_CARRYING_COST,
        ranked.INVENTORY_UPDATE_TS,
        ranked.OTIF_FLAG,
        ranked.LANDED_COST_PER_UNIT,
        ranked.DSI_ESTIMATE,
        ranked.GMROI,
        ranked.ORDER_TO_CASH_CYCLE_DAYS,
        ranked.RETURN_TO_SALES_RATIO,
        ranked.LAST_MILE_EFFICIENCY,
        ranked.STOCKOUT_RISK_SCORE,
        ranked.CARBON_FOOTPRINT_EST,
        ranked.CARRIER_LEAD_TIME_VARIANCE
    FROM (
        SELECT
            final_metrics.*,
            ROW_NUMBER() OVER (
                PARTITION BY final_metrics.SALES_ORDER_NO, final_metrics.PRODUCTID, final_metrics.SUPPLIERID
                ORDER BY final_metrics.ORDER_DATE DESC, final_metrics.ACTUAL_DELIVERY_DATE DESC
            ) AS rn
        FROM final_metrics
    ) ranked
    WHERE ranked.rn = 1
)
SELECT
    final_dataset.SALES_ORDER_NO,
    final_dataset.PRODUCTID,
    final_dataset.SUPPLIERID,
    final_dataset.CUSTOMERID,
    final_dataset.CUSTOMER_NAME,
    final_dataset.CUSTOMER_CITY,
    final_dataset.CUSTOMER_STATE,
    final_dataset.CUSTOMER_COUNTRY,
    final_dataset.PRODUCT_NAME,
    final_dataset.PRODUCT_COMPONENT,
    final_dataset.PRODUCT_EOL_DATE,
    final_dataset.SUPPLIER_NAME,
    final_dataset.SUPPLIER_CITY,
    final_dataset.SUPPLIER_STATE,
    final_dataset.SUPPLIER_COUNTRY,
    final_dataset.ROAD_TRANSPORT_COST,
    final_dataset.SEA_TRANSPORT_COST,
    final_dataset.AIR_TRANSPORT_COST,
    final_dataset.ORDER_DATE,
    final_dataset.EXPECTED_DELIVERY_DATE,
    final_dataset.ACTUAL_DELIVERY_DATE,
    final_dataset.SALES_QUANTITY,
    final_dataset.UNIT_PRICE,
    final_dataset.SELLING_PRICE,
    final_dataset.DISCOUNT_PER,
    final_dataset.FINAL_PRICE,
    final_dataset.MODE_OF_ORDER,
    final_dataset.MODE_OF_SHIPPING,
    final_dataset.SHIPPING_COST,
    final_dataset.SHIPPING_FACTOR,
    final_dataset.ON_TIME_SHIPPING_FLAG,
    final_dataset.ON_TIME_DELIVERY,
    final_dataset.ACCURATE_ORDER,
    final_dataset.ORDER_RETURN_FLAG,
    final_dataset.RETURN_QUANTITY,
    final_dataset.DEFECTIVE_UNIT,
    final_dataset.DEFECT_COUNT,
    final_dataset.INVENTORY_UNITS,
    final_dataset.IN_OUT_STOCK,
    final_dataset.NO_OF_DAYS_OUT_OF_STOCK,
    final_dataset.INVENTORY_SERVICE_COSTS,
    final_dataset.INVENTORY_RISK_COSTS,
    final_dataset.STORAGE_COST,
    final_dataset.INVENTORY_CARRYING_COST,
    final_dataset.INVENTORY_UPDATE_TS,
    final_dataset.OTIF_FLAG,
    final_dataset.LANDED_COST_PER_UNIT,
    final_dataset.DSI_ESTIMATE,
    final_dataset.GMROI,
    final_dataset.ORDER_TO_CASH_CYCLE_DAYS,
    final_dataset.RETURN_TO_SALES_RATIO,
    final_dataset.LAST_MILE_EFFICIENCY,
    final_dataset.STOCKOUT_RISK_SCORE,
    final_dataset.CARBON_FOOTPRINT_EST,
    final_dataset.CARRIER_LEAD_TIME_VARIANCE
FROM final_dataset;

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
            ) AS ORDER_DATE,
            COALESCE(
                TRY_TO_DATE(TO_VARCHAR(EXPECTED_DELIVERY_DATE), 'DD-MM-YYYY'),
                TRY_TO_DATE(TO_VARCHAR(EXPECTED_DELIVERY_DATE), 'YYYY-MM-DD'),
                TRY_TO_DATE(EXPECTED_DELIVERY_DATE)
            ) AS EXPECTED_DELIVERY_DATE,
            COALESCE(
                TRY_TO_DATE(TO_VARCHAR(ACTUAL_DELIVERY_DATE), 'DD-MM-YYYY'),
                TRY_TO_DATE(TO_VARCHAR(ACTUAL_DELIVERY_DATE), 'YYYY-MM-DD'),
                TRY_TO_DATE(ACTUAL_DELIVERY_DATE)
            ) AS ACTUAL_DELIVERY_DATE,
            SALES_QUANTITY,
            ACCURATE_ORDER,
            "RETURN" AS ORDER_RETURN_FLAG,
            CASE WHEN "RETURN" THEN 1 ELSE 0 END AS RETURN_QUANTITY,
            DEFECTIVE_UNIT,
            DEFECT_COUNT,
            MODE_OF_ORDER,
            SHIPPING_COST,
            ON_TIME_SHIPPING_FLAG,
            ON_TIME_DELIVERY,
            SELLING_PRICE,
            DISCOUNT_PER,
            FINAL_PRICE,
            MODE_OF_SHIPPING
        FROM AI_ETL.AI_ETL_STG.STG_ORDERS
    ),
    orders_dedup AS (
        SELECT
            orders_parsed.*,
            ROW_NUMBER() OVER (
                PARTITION BY orders_parsed.SALES_ORDER_NO, orders_parsed.PRODUCTID, orders_parsed.SUPPLIERID
                ORDER BY orders_parsed.ORDER_DATE DESC, orders_parsed.ACTUAL_DELIVERY_DATE DESC
            ) AS rn
        FROM orders_parsed
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
                TRY_TO_TIMESTAMP(TO_VARCHAR(INVENTORY_UPDATE_DATE), 'DD-MM-YYYY HH24:MI:SS'),
                TRY_TO_TIMESTAMP(TO_VARCHAR(INVENTORY_UPDATE_DATE), 'YYYY-MM-DD HH24:MI:SS'),
                TRY_TO_TIMESTAMP(TO_VARCHAR(INVENTORY_UPDATE_DATE), 'YYYY-MM-DD'),
                TRY_TO_TIMESTAMP(INVENTORY_UPDATE_DATE)
            ) AS INVENTORY_UPDATE_TS
        FROM AI_ETL.AI_ETL_STG.STG_INVENTORY
    ),
    inventory_dedup AS (
        SELECT
            inventory_parsed.*,
            ROW_NUMBER() OVER (
                PARTITION BY inventory_parsed.PRODUCT_ID, inventory_parsed.SUPPLIER_ID
                ORDER BY inventory_parsed.INVENTORY_UPDATE_TS DESC
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
            INVENTORY_UPDATE_TS
        FROM inventory_dedup
        WHERE rn = 1
    ),
    product_enriched AS (
        SELECT
            PRODUCT_ID,
            PRODUCT_NAME,
            PRODUCT_COMPONENT,
            UNIT_PRICE,
            COALESCE(
                TRY_TO_DATE(TO_VARCHAR(EOL_DATE), 'DD-MM-YYYY'),
                TRY_TO_DATE(TO_VARCHAR(EOL_DATE), 'YYYY-MM-DD'),
                TRY_TO_DATE(EOL_DATE)
            ) AS PRODUCT_EOL_DATE,
            ROW_NUMBER() OVER (
                PARTITION BY PRODUCT_ID
                ORDER BY PRODUCT_ID
            ) AS rn
        FROM AI_ETL.AI_ETL_STG.STG_PRODUCT
    ),
    product_latest AS (
        SELECT
            PRODUCT_ID,
            PRODUCT_NAME,
            PRODUCT_COMPONENT,
            UNIT_PRICE,
            PRODUCT_EOL_DATE
        FROM product_enriched
        WHERE rn = 1
    ),
    supplier_enriched AS (
        SELECT
            SUPPLIER_ID,
            SUPPLIER_NAME,
            SUPPLIER_CITY,
            STATE AS SUPPLIER_STATE,
            COUNTRY AS SUPPLIER_COUNTRY,
            ROAD_TRANSPORT_COST,
            SEA_TRANSPORT_COST,
            AIR_TRANSPORT_COST,
            ROW_NUMBER() OVER (
                PARTITION BY SUPPLIER_ID
                ORDER BY SUPPLIER_ID
            ) AS rn
        FROM AI_ETL.AI_ETL_STG.STG_SUPPLIER
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
        FROM supplier_enriched
        WHERE rn = 1
    ),
    customer_enriched AS (
        SELECT
            CUSTOMERID,
            CUSTOMER_NAME,
            CUSTOMER_CITY,
            CUSTOMER_STATE,
            CUSTOMER_COUNTRY,
            ROW_NUMBER() OVER (
                PARTITION BY CUSTOMERID
                ORDER BY CUSTOMERID
            ) AS rn
        FROM AI_ETL.AI_ETL_STG.STG_CUSTOMER
    ),
    customer_latest AS (
        SELECT
            CUSTOMERID,
            CUSTOMER_NAME,
            CUSTOMER_CITY,
            CUSTOMER_STATE,
            CUSTOMER_COUNTRY
        FROM customer_enriched
        WHERE rn = 1
    ),
    joined_data AS (
        SELECT
            ord.SALES_ORDER_NO,
            ord.PRODUCTID,
            ord.SUPPLIERID,
            ord.CUSTOMERID,
            cust.CUSTOMER_NAME,
            cust.CUSTOMER_CITY,
            cust.CUSTOMER_STATE,
            cust.CUSTOMER_COUNTRY,
            prod.PRODUCT_NAME,
            prod.PRODUCT_COMPONENT,
            prod.UNIT_PRICE,
            prod.PRODUCT_EOL_DATE,
            sup.SUPPLIER_NAME,
            sup.SUPPLIER_CITY,
            sup.SUPPLIER_STATE,
            sup.SUPPLIER_COUNTRY,
            sup.ROAD_TRANSPORT_COST,
            sup.SEA_TRANSPORT_COST,
            sup.AIR_TRANSPORT_COST,
            ord.ORDER_DATE,
            ord.EXPECTED_DELIVERY_DATE,
            ord.ACTUAL_DELIVERY_DATE,
            ord.SALES_QUANTITY,
            ord.ACCURATE_ORDER,
            ord.ORDER_RETURN_FLAG,
            ord.RETURN_QUANTITY,
            ord.DEFECTIVE_UNIT,
            ord.DEFECT_COUNT,
            ord.MODE_OF_ORDER,
            ord.SHIPPING_COST,
            ord.ON_TIME_SHIPPING_FLAG,
            ord.ON_TIME_DELIVERY,
            ord.SELLING_PRICE,
            ord.DISCOUNT_PER,
            ord.FINAL_PRICE,
            ord.MODE_OF_SHIPPING,
            inv.INVENTORY_UNITS,
            inv.IN_OUT_STOCK,
            inv.NO_OF_DAYS_OUT_OF_STOCK,
            inv.INVENTORY_SERVICE_COSTS,
            inv.INVENTORY_RISK_COSTS,
            inv.STORAGE_COST,
            inv.INVENTORY_CARRYING_COST,
            inv.INVENTORY_UPDATE_TS
        FROM orders_dedup ord
        LEFT JOIN inventory_latest inv
            ON ord.PRODUCTID = inv.PRODUCT_ID
            AND ord.SUPPLIERID = inv.SUPPLIER_ID
        LEFT JOIN product_latest prod
            ON ord.PRODUCTID = prod.PRODUCT_ID
        LEFT JOIN supplier_latest sup
            ON ord.SUPPLIERID = sup.SUPPLIER_ID
        LEFT JOIN customer_latest cust
            ON ord.CUSTOMERID = cust.CUSTOMERID
        WHERE ord.rn = 1
    ),
    final_base AS (
        SELECT
            joined_data.SALES_ORDER_NO,
            joined_data.PRODUCTID,
            joined_data.SUPPLIERID,
            joined_data.CUSTOMERID,
            joined_data.CUSTOMER_NAME,
            joined_data.CUSTOMER_CITY,
            joined_data.CUSTOMER_STATE,
            joined_data.CUSTOMER_COUNTRY,
            joined_data.PRODUCT_NAME,
            joined_data.PRODUCT_COMPONENT,
            joined_data.PRODUCT_EOL_DATE,
            joined_data.SUPPLIER_NAME,
            joined_data.SUPPLIER_CITY,
            joined_data.SUPPLIER_STATE,
            joined_data.SUPPLIER_COUNTRY,
            joined_data.ROAD_TRANSPORT_COST,
            joined_data.SEA_TRANSPORT_COST,
            joined_data.AIR_TRANSPORT_COST,
            joined_data.ORDER_DATE,
            joined_data.EXPECTED_DELIVERY_DATE,
            joined_data.ACTUAL_DELIVERY_DATE,
            joined_data.SALES_QUANTITY,
            joined_data.UNIT_PRICE,
            joined_data.SELLING_PRICE,
            joined_data.DISCOUNT_PER,
            joined_data.FINAL_PRICE,
            joined_data.MODE_OF_ORDER,
            joined_data.MODE_OF_SHIPPING,
            joined_data.SHIPPING_COST,
            joined_data.ON_TIME_SHIPPING_FLAG,
            joined_data.ON_TIME_DELIVERY,
            joined_data.ACCURATE_ORDER,
            joined_data.ORDER_RETURN_FLAG,
            joined_data.RETURN_QUANTITY,
            joined_data.DEFECTIVE_UNIT,
            joined_data.DEFECT_COUNT,
            joined_data.INVENTORY_UNITS,
            joined_data.IN_OUT_STOCK,
            joined_data.NO_OF_DAYS_OUT_OF_STOCK,
            joined_data.INVENTORY_SERVICE_COSTS,
            joined_data.INVENTORY_RISK_COSTS,
            joined_data.STORAGE_COST,
            joined_data.INVENTORY_CARRYING_COST,
            joined_data.INVENTORY_UPDATE_TS,
            CASE
                WHEN joined_data.MODE_OF_SHIPPING IS NULL THEN 1
                WHEN UPPER(joined_data.MODE_OF_SHIPPING) IN ('AIR', 'AIR FREIGHT') THEN 5
                WHEN UPPER(joined_data.MODE_OF_SHIPPING) IN ('SEA', 'SEA FREIGHT', 'OCEAN') THEN 2
                WHEN UPPER(joined_data.MODE_OF_SHIPPING) IN ('ROAD', 'GROUND', 'TRUCK') THEN 1.5
                WHEN UPPER(joined_data.MODE_OF_SHIPPING) = 'RAIL' THEN 1.2
                ELSE 1
            END AS SHIPPING_FACTOR
        FROM joined_data
    ),
    final_metrics AS (
        SELECT
            final_base.SALES_ORDER_NO,
            final_base.PRODUCTID,
            final_base.SUPPLIERID,
            final_base.CUSTOMERID,
            final_base.CUSTOMER_NAME,
            final_base.CUSTOMER_CITY,
            final_base.CUSTOMER_STATE,
            final_base.CUSTOMER_COUNTRY,
            final_base.PRODUCT_NAME,
            final_base.PRODUCT_COMPONENT,
            final_base.PRODUCT_EOL_DATE,
            final_base.SUPPLIER_NAME,
            final_base.SUPPLIER_CITY,
            final_base.SUPPLIER_STATE,
            final_base.SUPPLIER_COUNTRY,
            final_base.ROAD_TRANSPORT_COST,
            final_base.SEA_TRANSPORT_COST,
            final_base.AIR_TRANSPORT_COST,
            final_base.ORDER_DATE,
            final_base.EXPECTED_DELIVERY_DATE,
            final_base.ACTUAL_DELIVERY_DATE,
            final_base.SALES_QUANTITY,
            final_base.UNIT_PRICE,
            final_base.SELLING_PRICE,
            final_base.DISCOUNT_PER,
            final_base.FINAL_PRICE,
            final_base.MODE_OF_ORDER,
            final_base.MODE_OF_SHIPPING,
            final_base.SHIPPING_COST,
            final_base.SHIPPING_FACTOR,
            final_base.ON_TIME_SHIPPING_FLAG,
            final_base.ON_TIME_DELIVERY,
            final_base.ACCURATE_ORDER,
            final_base.ORDER_RETURN_FLAG,
            final_base.RETURN_QUANTITY,
            final_base.DEFECTIVE_UNIT,
            final_base.DEFECT_COUNT,
            final_base.INVENTORY_UNITS,
            final_base.IN_OUT_STOCK,
            final_base.NO_OF_DAYS_OUT_OF_STOCK,
            final_base.INVENTORY_SERVICE_COSTS,
            final_base.INVENTORY_RISK_COSTS,
            final_base.STORAGE_COST,
            final_base.INVENTORY_CARRYING_COST,
            final_base.INVENTORY_UPDATE_TS,
            CASE WHEN final_base.ON_TIME_DELIVERY = TRUE AND final_base.ACCURATE_ORDER = TRUE THEN 1 ELSE 0 END AS OTIF_FLAG,
            COALESCE(final_base.UNIT_PRICE, 0) + COALESCE(final_base.SHIPPING_COST, 0) + COALESCE(final_base.INVENTORY_SERVICE_COSTS, 0) + COALESCE(final_base.STORAGE_COST, 0) AS LANDED_COST_PER_UNIT,
            final_base.INVENTORY_UNITS / NULLIF(final_base.SALES_QUANTITY, 0) AS DSI_ESTIMATE,
            ((COALESCE(final_base.FINAL_PRICE, 0) - COALESCE(final_base.UNIT_PRICE, 0)) * COALESCE(final_base.SALES_QUANTITY, 0)) / NULLIF(COALESCE(final_base.UNIT_PRICE, 0) * COALESCE(final_base.INVENTORY_UNITS, 0), 0) AS GMROI,
            DATEDIFF('day', final_base.ORDER_DATE, final_base.ACTUAL_DELIVERY_DATE) AS ORDER_TO_CASH_CYCLE_DAYS,
            final_base.RETURN_QUANTITY / NULLIF(final_base.SALES_QUANTITY, 0) AS RETURN_TO_SALES_RATIO,
            COALESCE(final_base.SHIPPING_COST, 0) / NULLIF(final_base.SALES_QUANTITY, 0) AS LAST_MILE_EFFICIENCY,
            final_base.NO_OF_DAYS_OUT_OF_STOCK / NULLIF(final_base.SALES_QUANTITY, 0) AS STOCKOUT_RISK_SCORE,
            COALESCE(final_base.SALES_QUANTITY, 0) * final_base.SHIPPING_FACTOR AS CARBON_FOOTPRINT_EST,
            DATEDIFF('day', final_base.EXPECTED_DELIVERY_DATE, final_base.ACTUAL_DELIVERY_DATE) AS CARRIER_LEAD_TIME_VARIANCE
        FROM final_base
    ),
    final_dataset AS (
        SELECT
            ranked.SALES_ORDER_NO,
            ranked.PRODUCTID,
            ranked.SUPPLIERID,
            ranked.CUSTOMERID,
            ranked.CUSTOMER_NAME,
            ranked.CUSTOMER_CITY,
            ranked.CUSTOMER_STATE,
            ranked.CUSTOMER_COUNTRY,
            ranked.PRODUCT_NAME,
            ranked.PRODUCT_COMPONENT,
            ranked.PRODUCT_EOL_DATE,
            ranked.SUPPLIER_NAME,
            ranked.SUPPLIER_CITY,
            ranked.SUPPLIER_STATE,
            ranked.SUPPLIER_COUNTRY,
            ranked.ROAD_TRANSPORT_COST,
            ranked.SEA_TRANSPORT_COST,
            ranked.AIR_TRANSPORT_COST,
            ranked.ORDER_DATE,
            ranked.EXPECTED_DELIVERY_DATE,
            ranked.ACTUAL_DELIVERY_DATE,
            ranked.SALES_QUANTITY,
            ranked.UNIT_PRICE,
            ranked.SELLING_PRICE,
            ranked.DISCOUNT_PER,
            ranked.FINAL_PRICE,
            ranked.MODE_OF_ORDER,
            ranked.MODE_OF_SHIPPING,
            ranked.SHIPPING_COST,
            ranked.SHIPPING_FACTOR,
            ranked.ON_TIME_SHIPPING_FLAG,
            ranked.ON_TIME_DELIVERY,
            ranked.ACCURATE_ORDER,
            ranked.ORDER_RETURN_FLAG,
            ranked.RETURN_QUANTITY,
            ranked.DEFECTIVE_UNIT,
            ranked.DEFECT_COUNT,
            ranked.INVENTORY_UNITS,
            ranked.IN_OUT_STOCK,
            ranked.NO_OF_DAYS_OUT_OF_STOCK,
            ranked.INVENTORY_SERVICE_COSTS,
            ranked.INVENTORY_RISK_COSTS,
            ranked.STORAGE_COST,
            ranked.INVENTORY_CARRYING_COST,
            ranked.INVENTORY_UPDATE_TS,
            ranked.OTIF_FLAG,
            ranked.LANDED_COST_PER_UNIT,
            ranked.DSI_ESTIMATE,
            ranked.GMROI,
            ranked.ORDER_TO_CASH_CYCLE_DAYS,
            ranked.RETURN_TO_SALES_RATIO,
            ranked.LAST_MILE_EFFICIENCY,
            ranked.STOCKOUT_RISK_SCORE,
            ranked.CARBON_FOOTPRINT_EST,
            ranked.CARRIER_LEAD_TIME_VARIANCE
        FROM (
            SELECT
                final_metrics.*,
                ROW_NUMBER() OVER (
                    PARTITION BY final_metrics.SALES_ORDER_NO, final_metrics.PRODUCTID, final_metrics.SUPPLIERID
                    ORDER BY final_metrics.ORDER_DATE DESC, final_metrics.ACTUAL_DELIVERY_DATE DESC
                ) AS rn
            FROM final_metrics
        ) ranked
        WHERE ranked.rn = 1
    )
    SELECT
        *
    FROM final_dataset
) AS S
ON T.SALES_ORDER_NO = S.SALES_ORDER_NO
AND T.PRODUCTID = S.PRODUCTID
AND T.SUPPLIERID = S.SUPPLIERID
WHEN MATCHED THEN UPDATE SET
    T.CUSTOMERID = S.CUSTOMERID,
    T.CUSTOMER_NAME = S.CUSTOMER_NAME,
    T.CUSTOMER_CITY = S.CUSTOMER_CITY,
    T.CUSTOMER_STATE = S.CUSTOMER_STATE,
    T.CUSTOMER_COUNTRY = S.CUSTOMER_COUNTRY,
    T.PRODUCT_NAME = S.PRODUCT_NAME,
    T.PRODUCT_COMPONENT = S.PRODUCT_COMPONENT,
    T.PRODUCT_EOL_DATE = S.PRODUCT_EOL_DATE,
    T.SUPPLIER_NAME = S.SUPPLIER_NAME,
    T.SUPPLIER_CITY = S.SUPPLIER_CITY,
    T.SUPPLIER_STATE = S.SUPPLIER_STATE,
    T.SUPPLIER_COUNTRY = S.SUPPLIER_COUNTRY,
    T.ROAD_TRANSPORT_COST = S.ROAD_TRANSPORT_COST,
    T.SEA_TRANSPORT_COST = S.SEA_TRANSPORT_COST,
    T.AIR_TRANSPORT_COST = S.AIR_TRANSPORT_COST,
    T.ORDER_DATE = S.ORDER_DATE,
    T.EXPECTED_DELIVERY_DATE = S.EXPECTED_DELIVERY_DATE,
    T.ACTUAL_DELIVERY_DATE = S.ACTUAL_DELIVERY_DATE,
    T.SALES_QUANTITY = S.SALES_QUANTITY,
    T.UNIT_PRICE = S.UNIT_PRICE,
    T.SELLING_PRICE = S.SELLING_PRICE,
    T.DISCOUNT_PER = S.DISCOUNT_PER,
    T.FINAL_PRICE = S.FINAL_PRICE,
    T.MODE_OF_ORDER = S.MODE_OF_ORDER,
    T.MODE_OF_SHIPPING = S.MODE_OF_SHIPPING,
    T.SHIPPING_COST = S.SHIPPING_COST,
    T.SHIPPING_FACTOR = S.SHIPPING_FACTOR,
    T.ON_TIME_SHIPPING_FLAG = S.ON_TIME_SHIPPING_FLAG,
    T.ON_TIME_DELIVERY = S.ON_TIME_DELIVERY,
    T.ACCURATE_ORDER = S.ACCURATE_ORDER,
    T.ORDER_RETURN_FLAG = S.ORDER_RETURN_FLAG,
    T.RETURN_QUANTITY = S.RETURN_QUANTITY,
    T.DEFECTIVE_UNIT = S.DEFECTIVE_UNIT,
    T.DEFECT_COUNT = S.DEFECT_COUNT,
    T.INVENTORY_UNITS = S.INVENTORY_UNITS,
    T.IN_OUT_STOCK = S.IN_OUT_STOCK,
    T.NO_OF_DAYS_OUT_OF_STOCK = S.NO_OF_DAYS_OUT_OF_STOCK,
    T.INVENTORY_SERVICE_COSTS = S.INVENTORY_SERVICE_COSTS,
    T.INVENTORY_RISK_COSTS = S.INVENTORY_RISK_COSTS,
    T.STORAGE_COST = S.STORAGE_COST,
    T.INVENTORY_CARRYING_COST = S.INVENTORY_CARRYING_COST,
    T.INVENTORY_UPDATE_TS = S.INVENTORY_UPDATE_TS,
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
    PRODUCT_NAME,
    PRODUCT_COMPONENT,
    PRODUCT_EOL_DATE,
    SUPPLIER_NAME,
    SUPPLIER_CITY,
    SUPPLIER_STATE,
    SUPPLIER_COUNTRY,
    ROAD_TRANSPORT_COST,
    SEA_TRANSPORT_COST,
    AIR_TRANSPORT_COST,
    ORDER_DATE,
    EXPECTED_DELIVERY_DATE,
    ACTUAL_DELIVERY_DATE,
    SALES_QUANTITY,
    UNIT_PRICE,
    SELLING_PRICE,
    DISCOUNT_PER,
    FINAL_PRICE,
    MODE_OF_ORDER,
    MODE_OF_SHIPPING,
    SHIPPING_COST,
    SHIPPING_FACTOR,
    ON_TIME_SHIPPING_FLAG,
    ON_TIME_DELIVERY,
    ACCURATE_ORDER,
    ORDER_RETURN_FLAG,
    RETURN_QUANTITY,
    DEFECTIVE_UNIT,
    DEFECT_COUNT,
    INVENTORY_UNITS,
    IN_OUT_STOCK,
    NO_OF_DAYS_OUT_OF_STOCK,
    INVENTORY_SERVICE_COSTS,
    INVENTORY_RISK_COSTS,
    STORAGE_COST,
    INVENTORY_CARRYING_COST,
    INVENTORY_UPDATE_TS,
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
    S.PRODUCT_NAME,
    S.PRODUCT_COMPONENT,
    S.PRODUCT_EOL_DATE,
    S.SUPPLIER_NAME,
    S.SUPPLIER_CITY,
    S.SUPPLIER_STATE,
    S.SUPPLIER_COUNTRY,
    S.ROAD_TRANSPORT_COST,
    S.SEA_TRANSPORT_COST,
    S.AIR_TRANSPORT_COST,
    S.ORDER_DATE,
    S.EXPECTED_DELIVERY_DATE,
    S.ACTUAL_DELIVERY_DATE,
    S.SALES_QUANTITY,
    S.UNIT_PRICE,
    S.SELLING_PRICE,
    S.DISCOUNT_PER,
    S.FINAL_PRICE,
    S.MODE_OF_ORDER,
    S.MODE_OF_SHIPPING,
    S.SHIPPING_COST,
    S.SHIPPING_FACTOR,
    S.ON_TIME_SHIPPING_FLAG,
    S.ON_TIME_DELIVERY,
    S.ACCURATE_ORDER,
    S.ORDER_RETURN_FLAG,
    S.RETURN_QUANTITY,
    S.DEFECTIVE_UNIT,
    S.DEFECT_COUNT,
    S.INVENTORY_UNITS,
    S.IN_OUT_STOCK,
    S.NO_OF_DAYS_OUT_OF_STOCK,
    S.INVENTORY_SERVICE_COSTS,
    S.INVENTORY_RISK_COSTS,
    S.STORAGE_COST,
    S.INVENTORY_CARRYING_COST,
    S.INVENTORY_UPDATE_TS,
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