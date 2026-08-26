{{
  config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'customer_id'
    )
}}

SELECT
    *,
    CURRENT_TIMESTAMP() AS processed_at
FROM
    {{ source('tesco_databricks', 'customers') }}

{% if is_incremental() %}

    WHERE updated_timestamp > (
        SELECT COALESCE(
            MAX(updated_timestamp),
            '1900-01-01'
        )
        FROM {{ this }}
    )

{% endif %}