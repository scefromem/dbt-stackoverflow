{{ config(schema = 'stackoverflow_silver') }}

with source as (
    select
  id as user_id,
  age,
  creation_date,
  round(timestamp_diff(current_timestamp(), creation_date, day)/365) as user_tenure
from
 {{ source('bronze', 'users') }}
)

select * from source
