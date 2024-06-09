with
    headers as (
        select
            announcement_id,
            announcement_url,
            announcement_date,
            announcement_category,
            company_name,
            security_name
        from {{ ref("stg_sg_sgx__announcements") }}
    ),

    details as (
        select
            announcement_id,
            announcement_reference,
            broadcast_at_utc,
            broadcast_at_local_time,
            local_time_zone,
            issuer_name,
            security_name,
            submitted_by_name
            announcement_status,
            announcement_category,
            announcement_category_details,
            announcement_description
        from {{ ref("stg_sg_sgx__announcements_details") }}
    ),

    joined as (
        select
            -- identifiers
            headers.announcement_id,
            details.announcement_reference,
            headers.announcement_url,

            -- categories
            coalesce(headers.announcement_category, details.announcement_category) as announcement_category,

            -- dates
            headers.announcement_date,
            details.broadcast_at_utc,
            details.broadcast_at_local_time,
            details.local_time_zone,

            -- company/security
            coalesce(headers.company_name, details.issuer_name) as company_name,
            details.security_name as security_name_detail,

            -- description
            details.announcement_description

        from headers
        left join details
            on headers.announcement_id = details.announcement_id
    )

select *
from joined