select order_id,
    o.customer_id,
    c.city,
    c.state,
    order_status,
    order_date,
    store_id,
    staff_id,
    sum(item_quantity) as total_item_quantity,
    sum(total_order_item_amount) as total_order_amount
from {{ ref('int_bike_database__order_items') }} AS o
left join {{ ref('stg_bike_database__customers') }} as c ON c.customer_id = o .customer_id
group by order_id,
    customer_id,
    city,
    state,
    order_status,
    order_date,
    store_id,
    staff_id