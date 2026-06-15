with products_solds as (

select store_id, 
    product_id, 
    sum(item_quantity) AS total_item_solds,
    SUM(total_order_item_amount) AS total_sales_amount
FROM {{ ref('int_bike_database__order_items') }}
group by store_id, 
    product_id
)


SELECT  stocks.store_id, 
    stocks.product_id, 
    stocks.store_name,
    stocks.product_name, 
    stocks.stock_quantity,
    products_solds.total_item_solds,
    products_solds.total_sales_amount

FROM {{ ref('int_bike_database__stocks') }} AS stocks
left join products_solds on stocks.store_id = products_solds.store_id and stocks.product_id = products_solds.product_id
