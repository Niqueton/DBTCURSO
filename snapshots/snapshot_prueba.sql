{% snapshot budget_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='_row',
      strategy='check',
      check_cols=['quantity'],
      invalidate_hard_deletes=True
        )
}}

select * from {{ source('src_google_sheets', 'budget') }}

{% endsnapshot %}