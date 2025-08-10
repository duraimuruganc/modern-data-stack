with raw_posts as (
  select
    try_to_number(userId) as user_id,
    try_to_number(id)     as post_id,
    cast(title as string) as title,
    cast(body  as string) as body
  from {{ source('raw','POSTS') }}
  qualify row_number() over (partition by id order by id) = 1
)
select * from raw_posts