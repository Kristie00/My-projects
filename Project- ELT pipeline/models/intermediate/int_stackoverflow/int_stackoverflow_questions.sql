with

data_from_stg_stackoverflow_questions as(
    select
        _pk,
        title,
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
        tags_unnested,
        view_count
    from {{ source('stg_stackoverflow_questions2','stg_stackoverflow_questions2') }}


    {% if is_incremental() %}
        where creation_date >= (select max(creation_date) from {{ this }})
    {% endif %}
),

data_from_int_gsheet as(
    select
        repository_account,
        repository_name,
        organization as organization_name,
        tags_unnested
    from {{ source('int_gsheet2','int_gsheet2') }}
    where valid_to_datetime_utc is null and tags_unnested is not null
),

final_table as(
    select
        so._pk,
        so.title,
        so.accepted_answer_id,
        so.answer_count,
        so.comment_count,
        so.community_owned_date,
        so.creation_date as creation_datetime_utc,
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
    from data_from_stg_stackoverflow_questions so
    right join data_from_int_gsheet gs
    on so.tags_unnested = gs.tags_unnested
    where so.tags_unnested = gs.tags_unnested
)

select * from final_table