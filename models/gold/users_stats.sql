select
  stg_users.user_id,
  stg_users.age,
  stg_users.creation_date,
  stg_users.user_tenure,
  count(distinct stg_badges.badge_id) as badge_count,
  count(distinct posts_all.post_id) as questions_and_answer_count,
  count(distinct if(type = "question", posts_all.post_id, null)) as question_count,
  count(distinct if(type = "answer", posts_all.post_id, null)) as answer_count,
  max(stg_badges.award_timestamp) as last_badge_received_at,
  max(posts_all.created_at) as last_posted_at,
  max(if(type = "question", posts_all.created_at, null)) as last_question_posted_at,
  max(if(type = "answer", posts_all.created_at, null)) as last_answer_posted_at
from
  {{ ref('stg_users') }} as stg_users
  left join {{ ref('stg_badges') }} as stg_badges
    on stg_users.user_id = stg_badges.user_id
  left join {{ ref('posts_combined') }} as posts_all
    on stg_users.user_id = posts_all.owner_user_id
group by
  1,2,3,4
