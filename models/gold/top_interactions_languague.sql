with tmp_query_clean as (
SELECT *
  FROM (
    SELECT post_id, EXTRACT(YEAR FROM created_at) year, SPLIT(tags, '|') tags, accepted_answer_id, created_at 
    FROM {{ ref('stg_posts_questions') }}
  ), UNNEST(tags) tag
  WHERE accepted_answer_id IS NOT null
)

SELECT tag, COUNT(*) c, COUNT(DISTINCT b.owner_user_id) answerers, AVG(TIMESTAMP_DIFF(b.created_at,a.created_at, MINUTE)) time_to_answer
FROM tmp_query_clean a
LEFT JOIN {{ ref('stg_posts_answers') }} b
ON a.accepted_answer_id = b.post_id
GROUP BY 1
HAVING c > 300
ORDER BY 2 DESC
LIMIT 1000