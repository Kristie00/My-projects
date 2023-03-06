with

question as (
    select *
    from {{ source('Q','posts_questions') }},
    unnest(split(tags,'|')) AS tags_unnested
),

staged as(
    select
        id as _pk,
        title,
        body,
        accepted_answer_id,
        answer_count,
        comment_count,
        community_owned_date,
        creation_date,
        favorite_count,
        last_activity_date,
        last_edit_date,
        last_editor_display_name,
        last_editor_user_id,
        owner_display_name,
        owner_user_id,
        parent_id,
        post_type_id,
        score,
        tags,
        tags_unnested,
        view_count
    from question
    where EXTRACT(YEAR FROM creation_date)={{ var("year") }}
)

select * from staged