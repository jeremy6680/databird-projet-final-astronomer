select oi.order_id,
    oi.order_item_id,
    o.customer_id,
    o.order_status,
    o.order_date,
    o.store_id,
    o.staff_id,
    oi.product_id,
    item_quantity,
    item_price,
    discount,
    total_order_item_amount
from {{ ref('stg_bike_database__order_items') }} AS oi
left join {{ ref('stg_bike_database__orders') }} AS o ON o.order_id = oi.order_id 
