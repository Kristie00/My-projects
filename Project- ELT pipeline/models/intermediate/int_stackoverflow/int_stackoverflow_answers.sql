with

data_from_stg_stackoverflow_answers as(
    select
        a._pk,
        a._pk as id,
        a.title,
        a.accepted_answer_id,
        a.answer_count,
        a.comment_count,
        a.community_owned_date,
        a.creation_date,
        a.favorite_count,
        a.last_activity_date,
        a.last_edit_date,
        a.last_editor_display_name,
        a.last_editor_user_id,
        a.owner_display_name,
        a.owner_user_id,
        a.parent_id,
        a.post_type_id,
        a.score,
        q.tags_unnested,
        a.view_count
    from {{ source('stg_stackoverflow_answers2','stg_stackoverflow_answers2') }} a
    left join {{ref('int_stackoverflow_questions') }} q
    on a.parent_id = q._pk


    {% if is_incremental() %}
        where a.creation_date >= (select max(a.creation_date) from {{ this }})
    {% endif %}
),

data_from_int_gsheet as(
    select
        g.repository_account,
        g.repository_name,
        g.organization as organization_name,
        g.tags_unnested
    from {{ source('int_gsheet2','int_gsheet2') }} g
    where g.valid_to_datetime_utc is null
),

final_table as(
    select
        so._pk,
        so.title,
        so.accepted_answer_id,
        so.answer_count,
        so.comment_count,
        so.community_owned_date,
        so.creation_date,
        so.favorite_count,
        so.last_activity_date,
        so.last_edit_date,
        so.last_editor_display_name,
        so.last_editor_user_id,
        so.owner_display_name,
        so.owner_user_id,
        so.parent_id,
        so.post_type_id,
        so.score,
        so.tags_unnested,
        so.view_count,
        gs.repository_account,
        gs.repository_name,
        gs.organization_name
    from data_from_stg_stackoverflow_answers so
    right join data_from_int_gsheet gs
    on so.tags_unnested = gs.tags_unnested
    where so.tags_unnested = gs.tags_unnested
)

select * from final_table