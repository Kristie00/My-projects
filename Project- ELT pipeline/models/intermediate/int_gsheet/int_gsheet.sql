{{
    config(
        materialized='table',
        unique_key=['Organization']
    )
}}

with

first_source as (
    select *
    from {{ source('google_snapshot','gs_snapshot') }},
    UNNEST(split(tags,',')) AS tags_unnested
),

staged as (
    select
        Organization as `_pk`,
        tags_unnested,
        L1_type AS `l1_type`,
	    L2_type AS `l2_type`,
        L3_type	AS `l3_type`,
        Organization as organization,
        Repository_name AS `repository_name`,
        Repository_account AS `repository_account`,

            case Open_source_available
                when 'Yes' then 'True'
                ELSE 'False'
            end as is_open_source_available,

        _airbyte_emitted_at AS `airbyte_emitted_at`,
        _airbyte_normalized_at AS `airbyte_normalized_at`,
        _airbyte_List_1_hashid AS `airbyte_hash_id`,
        dbt_scd_id,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to AS valid_to_datetime_utc
    from first_source
)
select *
from staged