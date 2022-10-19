select
  id as post_id,
  creation_date as created_at,
  'question' as type,
  title,
  body,
  owner_user_id,
  parent_id
from {{ source('bronze', 'posts_questions') }}
where
  -- limit to recent data for the purposes of this demo project
  creation_date >= timestamp("2019-01-01")