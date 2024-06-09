with
    source as (select * from {{ source('sg_sgx', 'announcement_attachments') }}),
    renamed as (
        select
            {{ adapter.quote("announcement_reference") }},
            {{ adapter.quote("url") }} as announcement_url,
            {{ adapter.quote("attachment_name") }},
            {{ adapter.quote("attachment_link") }},

        from source
    )
select *
from renamed
