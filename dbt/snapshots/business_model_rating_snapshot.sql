{% snapshot business_model_rating_snapshot %}

{{
    config(
      target_database='financial_data',
      target_schema='snapshots',
      unique_key='ticker',
      strategy='timestamp',
      updated_at='_airbyte_emitted_at',
    )
}}

select * from {{ source('source', 'business_model_rating') }}

{% endsnapshot %}