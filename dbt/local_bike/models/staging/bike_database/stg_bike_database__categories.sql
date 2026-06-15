select category_id, 
  category_name
from {{ source('bike_database', 'categories') }}