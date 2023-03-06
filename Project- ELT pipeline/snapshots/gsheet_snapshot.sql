{% snapshot gsheet_snapshot %}

    {{
        config(
        target_database='nodal-wall-370813',
        target_schema='dbt_kkafkova',
        unique_key='organization',

        strategy='check',
        check_cols=['Organization', 'Repository_name', 'Repository_account', 'Tags', 'L1_type', 'L2_type', 'L3_type', 'Open_source_available'],
        invalidate_hard_deletes=True,
        )
    }}

    select * from {{ source('google_sheets','gs') }}

{% endsnapshot %}

