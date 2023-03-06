with

    data_from_stg_github as (
        select
            _pk,
            split(repo_name, '/')[offset(0)] as repository_account,
            split(repo_name, '/')[offset(1)] as repository_name,
            actor_id as user_id,
            id as event_id,
            type,
            created_at_datetime_utc
        from {{ source("stg_github", "stg_github") }}

        {% if is_incremental() %}

        where
            created_at_datetime_utc
            >= (select max(created_at_datetime_utc) from {{ this }})

        {% endif %}
    ),

    data_from_int_gsheet as (
        select repository_account, repository_name, organization as organization_name
        from {{ source("int_gsheet", "int_gsheet") }}
        where valid_to_datetime_utc is null and repository_account is not null
    ),

    final_table as (
        select distinct
            gh._pk,
            gh.repository_account,
            gh.repository_name,
            gh.user_id,
            gh.event_id,
            gh.type,
            gs.organization_name,
            gh.created_at_datetime_utc

        from data_from_stg_github gh
        right join
            data_from_int_gsheet gs
            on gh.repository_account = gs.repository_account
            and (gs.repository_name = gh.repository_name or gs.repository_name is null)
        where gs.repository_account is not null and _pk is not null
    )

select *
from final_table
