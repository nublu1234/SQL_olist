
WITH order_items AS (
    SELECT order_id, COUNT(order_id) AS order_quantity, MIN(shipping_limit_date) AS min_shipping_limit_date
          ,SUM(price) AS purchase_amount, SUM(freight_value) AS sum_freight_value
    FROM ecommerce_by_olist.olist_order_items_dataset 
    GROUP BY order_id
)
-- SELECT * FROM order_items LIMIT 5;
-- SELECT order_id, COUNT(order_id) order_id_cnt FROM order_items GROUP BY order_id HAVING COUNT(order_id) > 1 LIMIT 5;
-- order_id 중목 0

, orders AS (
    SELECT order_id, customer_id, order_status
          ,order_purchase_timestamp, order_approved_at
          ,order_delivered_carrier_date, order_delivered_customer_date
          ,order_estimated_delivery_date
    FROM ecommerce_by_olist.olist_orders_dataset 
--    WHERE order_status = 'delivered'
)
-- SELECT * FROM orders LIMIT 5;
-- SELECT order_id, COUNT(order_id) order_id_cnt FROM orders GROUP BY order_id HAVING COUNT(order_id) > 1 LIMIT 5;
/*
SELECT DISTINCT EXTRACT(YEAR FROM order_delivered_customer_date) order_completed_year, 
       EXTRACT(MONTH FROM order_delivered_customer_date) order_completed_month
FROM orders
ORDER BY order_completed_year, order_completed_month;
*/
-- order_id 중목 0

, customers AS (
    SELECT customer_unique_id, customer_id
    FROM ecommerce_by_olist.olist_customers_dataset
)
-- SELECT * FROM customers LIMIT 5;

, order_payments AS (
    SELECT order_id, SUM(payment_value) AS payment
    FROM ecommerce_by_olist.olist_order_payments_dataset
    GROUP BY order_id
)

, order_rewiews AS (
    SELECT order_id, AVG(review_score) AS avg_review_score
    FROM ecommerce_by_olist.olist_order_reviews_dataset
    GROUP BY order_id
)


--- 작성 다시
          
, order_all AS (
    SELECT o.order_id, o.customer_id, o.order_status
          ,o.order_purchase_timestamp, o.order_approved_at
          ,o.order_delivered_carrier_date, o.order_delivered_customer_date
--          ,EXTRACT(YEAR FROM o.order_delivered_customer_date) order_completed_year, 
--          ,EXTRACT(MONTH FROM o.order_delivered_customer_date) order_completed_month, 
          ,o.order_estimated_delivery_date
          ,c.customer_unique_id
          ,oi.order_quantity, oi.purchase_amount
          ,oi.min_shipping_limit_date, oi.sum_freight_value
          ,op.payment
          ,or_.avg_review_score
    FROM orders AS o
    LEFT JOIN order_items AS oi
    ON o.order_id = oi.order_id
    LEFT JOIN customers AS c
    ON o.customer_id = c.customer_id
    LEFT JOIN order_payments AS op
    ON o.order_id = op.order_id
    LEFT JOIN order_rewiews AS or_
    ON o.order_id = or_.order_id
)
-- SELECT * FROM order_all;

, customer_all AS (
    SELECT customer_unique_id
          ,MIN(order_purchase_timestamp) AS first_order_purchase_timestamp
          ,MAX(order_purchase_timestamp) AS latest_order_purchase_timestamp
          ,COUNT(order_id) AS all_order_cnt
          ,COUNT(purchase_amount) AS all_purchase_amount
          ,COUNT(avg_review_score) AS all_avg_review_score
          ,CASE WHEN CAST(MIN(order_purchase_timestamp) AS DATE)
                     BETWEEN PARSE_DATE("%Y%m%d", @DS_START_DATE) 
                     AND PARSE_DATE("%Y%m%d", @DS_END_DATE) THEN 1
                     ELSE 0 END AS first_order_customer
          ,CASE WHEN CAST(MIN(order_purchase_timestamp) AS DATE)
                     BETWEEN PARSE_DATE("%Y%m%d", @DS_START_DATE) 
                     AND PARSE_DATE("%Y%m%d", @DS_END_DATE) THEN 0
                     ELSE 1 END AS existing_order_customer
    FROM order_all
    WHERE CAST(order_purchase_timestamp AS DATE) 
          BETWEEN PARSE_DATE("%Y%m%d", @DS_START_DATE) 
          AND PARSE_DATE("%Y%m%d", @DS_END_DATE)
    GROUP BY customer_unique_id
)
-- SELECT * FROM customer_all WHERE first_order_purchase_timestamp <> latest_order_purchase_timestamp;
SELECT * FROM customer_all;