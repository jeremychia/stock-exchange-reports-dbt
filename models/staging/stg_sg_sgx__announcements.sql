with
    source as (select * from {{ source('sg_sgx', 'announcements') }}),
    renamed as (
        select
            cast({{ adapter.quote("document_date") }} as date) as document_date,
            trim({{ adapter.quote("security_name") }}) as security_name,
            trim({{ adapter.quote("company_name") }}) as company_name,
            trim({{ adapter.quote("id") }}) as announcement_id,
            trim({{ adapter.quote("title") }}) as announcement_category,
            trim({{ adapter.quote("url") }}) as announcement_url

        from source
    )
select *
from renamed
