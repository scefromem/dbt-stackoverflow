SELECT 
  EXTRACT(YEAR FROM created_at) year_question, 
  IF(title LIKE '%?', 'ends with ?', 'does not') ends_with_question,
  ROUND(COUNT(accepted_answer_id )* 100/COUNT(*), 2) as answered,
  ROUND(AVG(answer_count), 3) as avg_answers 
FROM {{ ref('stg_posts_questions') }}
WHERE created_at < (SELECT TIMESTAMP_SUB(MAX(created_at), INTERVAL 24*90 HOUR) FROM {{ ref('stg_posts_questions') }} )
GROUP BY 1,2
ORDER BY 1,2