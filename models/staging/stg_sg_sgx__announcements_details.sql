with
    source as (select * from {{ source('sg_sgx', 'announcement_details') }}),
    renamed as (
        select
            trim({{ adapter.quote("announcement_id") }}) as announcement_id,
            trim(
                {{ adapter.quote("announcement_reference") }}
            ) as announcement_reference,
            trim({{ adapter.quote("url") }}) as announcement_url,
            cast(
                {{ adapter.quote("financial_period_end") }} as date
            ) as financial_period_end,
            {{ adapter.quote("issuer") }} as issuer_name,
            {{ adapter.quote("securities") }} as security_name,
            case
                when lower({{ adapter.quote("is_stapled_security") }}) = 'yes'
                then true
                when lower({{ adapter.quote("is_stapled_security") }}) = 'no'
                then false
            end as is_stapled_security,
            {{ adapter.quote("broadcast_at") }} as broadcast_at_utc,
            datetime(
                {{ adapter.quote("broadcast_at") }}, "Asia/Singapore"
            ) as broadcast_at_local_time,
            'Asia/Singapore' as local_time_zone,
            {{ adapter.quote("status") }} as announcement_status,
            {{ adapter.quote("report_type") }} as announcement_category,
            {{ adapter.quote("announcement_title") }} as announcement_category_details,
            {{ adapter.quote("announcement_description") }} as announcement_description,
            {{ adapter.quote("submitted_by_name") }} as submitted_by_name,
            {{ adapter.quote("submitted_by_designation") }} as submitted_by_designation,

        from source
    )
select *
from renamed
