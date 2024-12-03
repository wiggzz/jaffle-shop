WITH stg_orders AS (
  SELECT
    ORDER_ID,
    LOCATION_ID,
    CUSTOMER_ID,
    SUBTOTAL_CENTS,
    TAX_PAID_CENTS,
    ORDER_TOTAL_CENTS,
    SUBTOTAL,
    TAX_PAID,
    ORDER_TOTAL,
    ORDERED_AT
  FROM {{ ref('stg_orders') }}
), stg_locations AS (
  SELECT
    LOCATION_ID,
    LOCATION_NAME,
    TAX_RATE,
    OPENED_DATE
  FROM {{ ref('stg_locations') }}
), stg_customers AS (
  SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME
  FROM {{ ref('stg_customers') }}
), join_1 AS (
  SELECT
    stg_customers.CUSTOMER_ID,
    stg_customers.CUSTOMER_NAME,
    stg_orders.ORDER_ID,
    stg_orders.LOCATION_ID
  FROM stg_customers
  JOIN stg_orders
    ON stg_customers.CUSTOMER_ID = stg_orders.CUSTOMER_ID
), join_2 AS (
  SELECT
    join_1.CUSTOMER_NAME,
    join_1.ORDER_ID,
    join_1.LOCATION_ID,
    stg_locations.LOCATION_ID AS LOCATION_ID_1,
    stg_locations.LOCATION_NAME
  FROM join_1
  JOIN stg_locations
    ON join_1.LOCATION_ID = stg_locations.LOCATION_ID
), aggregation_1 AS (
  SELECT
    CUSTOMER_NAME,
    COUNT(DISTINCT LOCATION_ID) AS countd_LOCATION_ID,
    COUNT(DISTINCT ORDER_ID) AS countd_ORDER_ID
  FROM join_2
  GROUP BY
    CUSTOMER_NAME
)
SELECT
  *
FROM aggregation_1