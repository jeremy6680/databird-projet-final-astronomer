select category_id, 
  category_name
from {{ source('bike_database_astronomer', 'categories') }}