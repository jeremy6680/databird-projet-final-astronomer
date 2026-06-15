SELECT stocks.stock_id,
    stocks.store_id, 
    stores.store_name,
    stocks.product_id, 
    products.product_name,
    stocks.stock_quantity
FROM {{ ref('stg_bike_database__stocks') }} AS stocks
left join {{ ref('stg_bike_database__stores') }} AS stores on stocks.store_id = stores.store_id
left join {{ ref('stg_bike_database__products') }} AS products on products.product_id = stocks.product_id