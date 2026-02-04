SELECT o.order_id, c.customer_name, p.payment_status
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN payments p ON o.order_id = p.order_id
WHERE p.payment_status = 'Failed';