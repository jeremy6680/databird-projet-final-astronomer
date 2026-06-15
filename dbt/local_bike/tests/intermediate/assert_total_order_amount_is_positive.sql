select
    order_id,
    sum(total_order_item_amount) as total_order_amount
from {{ ref('int_bike_database__order_items') }}
group by 1
having total_order_amount < 0