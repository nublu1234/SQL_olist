
WITH order_items1 AS (
    SELECT *, CONCAT(order_id, product_id) AS order_product_id
    FROM ecommerce_by_olist.olist_order_items_dataset
)

, order_items1_count AS (
    SELECT order_product_id, COUNT(order_product_id) AS order_product_cnt
    FROM order_items1
    GROUP BY order_product_id
)

, order_items1_distinct AS (
    SELECT DISTINCT order_product_id, order_id, product_id, seller_id, shipping_limit_date, price, freight_value
    FROM order_items1
)

, order_items2 AS (
    SELECT oid.order_product_id, oid.order_id, oid.product_id, oid.seller_id, oid.shipping_limit_date, oid.price, oid.freight_value
          ,CASE WHEN oic.order_product_cnt IS NULL THEN 1
                ELSE oic.order_product_cnt END AS order_product_cnt
    FROM order_items1_distinct AS oid
    LEFT JOIN order_items1_count AS oic
    ON oid.order_product_id = oic.order_product_id
)

-- SELECT * 
-- FROM order_items2 
-- WHERE order_id IN (SELECT order_id FROM order_items2 GROUP BY order_id HAVING COUNT(order_id) > 1)
-- ORDER BY order_id;


, orders AS (
    SELECT order_id, customer_id, order_status
          ,order_purchase_timestamp, order_approved_at
          ,order_delivered_carrier_date, order_delivered_customer_date
          ,order_estimated_delivery_date
    FROM ecommerce_by_olist.olist_orders_dataset 
--    WHERE order_status = 'delivered'
)
-- SELECT order_id, COUNT(order_id) order_id_cnt FROM orders GROUP BY order_id HAVING COUNT(order_id) > 1;


, customers AS (
    SELECT customer_unique_id, customer_id
    FROM ecommerce_by_olist.olist_customers_dataset
)
-- SELECT * FROM customers;

, order_payments AS (
    SELECT order_id, SUM(payment_value) AS payment
    FROM ecommerce_by_olist.olist_order_payments_dataset
    GROUP BY order_id
)

-- , order_rewiews AS (
--     SELECT order_id, AVG(review_score) AS avg_review_score
--     FROM ecommerce_by_olist.olist_order_reviews_dataset
--     GROUP BY order_id
-- )
-- 2회 이상의 주문 건에 1건의 동일한 리뷰가 존재하는 케이스가 있어 제외


, olist_products AS (
    SELECT product_id, pcnt.string_field_1 AS product_category_name_tr
    FROM ecommerce_by_olist.olist_products_dataset AS opd
    LEFT JOIN ecommerce_by_olist.product_category_name_translation AS pcnt
    ON opd.product_category_name = pcnt.string_field_0 
)


, ordered_items_all AS (
    SELECT oi2.order_id, oi2.product_id, oi2.seller_id
          ,oi2.shipping_limit_date, oi2.price, oi2.freight_value, oi2.order_product_cnt
          ,olp.product_category_name_tr
          ,o.customer_id, o.order_status
          ,o.order_purchase_timestamp, o.order_approved_at
          ,o.order_delivered_carrier_date, o.order_delivered_customer_date
          ,o.order_estimated_delivery_date
--           ,or_.avg_review_score
    FROM order_items2 AS oi2
    LEFT JOIN olist_products AS olp
    ON oi2.product_id = olp.product_id
    LEFT JOIN orders AS o
    ON oi2.order_id = o.order_id
--    LEFT JOIN order_rewiews AS or_
--    ON oi2.order_id = or_.order_id
)
-- SELECT * FROM ordered_items_all ORDER BY order_id;
SELECT * FROM ordered_items_all;
