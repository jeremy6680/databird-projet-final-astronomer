with sales_grouped_by_product AS (

select
    product_id,
    SUM(total_sales_amount) AS total_sales_amount,
    SUM(total_item_solds) AS total_item_solds
from {{ ref('mrt_operations__store_product_stock_summary') }} as oi
group by product_id

)

SELECT
    products.product_id,
    products.product_name,
    products.category_name,
    products.brand_name,
    sales.total_sales_amount,
    sales.total_item_solds
    
FROM {{ ref('int_bike_database__products') }} as products
left join sales_grouped_by_product as sales ON sales.product_id = products.product_id
