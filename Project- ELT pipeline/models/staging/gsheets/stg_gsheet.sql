with

source as (
    select *
    from {{ source('google_sheets','gs') }}
),

staged as (
    select
        Organization as `_pk`,
        Tags as tags,
        split(Tags, ',') as tags_split,
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
        _airbyte_List_1_hashid AS `airbyte_hash_id`
    from source
)
select *
from staged
