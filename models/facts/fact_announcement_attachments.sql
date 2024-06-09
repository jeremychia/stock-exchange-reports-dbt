-- because one announcement can have more than on attachment

with attachments as (
    select
        announcement_reference,
        attachment_name,
        attachment_link,
    from {{ ref("stg_sg_sgx__announcements_attachments") }}
),

details as (
    select
        announcement_reference,
        announcement_url,
        announcement_category
    from {{ ref("stg_sg_sgx__announcements_details") }}
),

joined as (
    select
        details.announcement_reference,
        details.announcement_url,
        details.announcement_category,
        attachments.attachment_name,
        attachments.attachment_link
    from details
    left join attachments
        on details.announcement_reference = attachments.announcement_reference
)

select *
from joined
