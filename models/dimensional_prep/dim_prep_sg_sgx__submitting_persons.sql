with
    details as (
        select
            submitted_by_name,
            issuer_name as company_name,
            lower(submitted_by_designation) as submitted_by_designation,
            cast(broadcast_at_local_time as date) broadcast_date
        from {{ ref("stg_sg_sgx__announcements_details") }}
    ),

    clean_names as (
        select
            *,
            case
                when submitted_by_name = 'YE TIANYUN'
                then 'Ye Tianyun'
                when submitted_by_name = 'ONG BENG HONG'
                then 'Ong Beng Hong'
                when submitted_by_name = 'LEE WEI HSIUNG'
                then 'Lee Wei Hsiung'
                when submitted_by_name = 'DR CHAN KUM LOK COLIN'
                then 'DR COLIN CHAN KUM LOK'
                when
                    submitted_by_name in (
                        'Mailene de la Torre/Aboitiz Equity Ventures Inc.',
                        'ABOITIZ EQUITY VENTURES INC/MAILENE DE LA TORRE',
                        'Aboitiz Equity Ventures Inc/Mailene de la Torre',
                        'Aboitiz Equity Venture Inc/Mailene de la Torre'
                    )
                then 'Aboitiz Equity Ventures Inc./Mailene de la Torre'
                when submitted_by_name = 'JOANNE LOH'
                then 'Joanne Loh'
                when submitted_by_name = 'Mr. William Ng'
                then 'William Ng'
                when submitted_by_name = 'Kyle Arnold Shaw Jr'
                then 'Kyle Arnold Shaw, Jr.'
                when submitted_by_name = ''
                then ''
                else replace(submitted_by_name, chr(160), ' ')
            end submitted_by_name_standardised
        from details
    ),

    summarise as (
        select
            lower(submitted_by_name_standardised) as submitted_by_name_standardised,
            array_agg(distinct submitted_by_name) as submitted_by_names,
            company_name,
            submitted_by_designation,
            min(broadcast_date) as effective_from,
            max(broadcast_date) as effective_to
        from clean_names
        group by all
    )

select 'sg_sgx' as stock_market, *
from summarise
