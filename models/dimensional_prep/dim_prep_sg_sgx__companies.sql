with
    details as (
        select
            issuer_name as company_name,
            security_name,
            case
                when announcement_category = 'Annual Report'
                then substr(cast(financial_period_end as string), 6, 5)
                else null
            end as financial_period_end_mm_dd,
            cast(broadcast_at_local_time as date) as announcement_date,
        from {{ ref("stg_sg_sgx__announcements_details") }}
    ),

    summarise as (
        select
            company_name,
            security_name,
            case
                when financial_period_end_mm_dd = '02-29'
                then '02-28'
                else financial_period_end_mm_dd
            end as financial_period_end_mm_dd,
            min(announcement_date) as effective_from,
            max(announcement_date) as effective_to,
        from details
        group by all
    ),

    filter_out_null_summary as (
        select
            company_name,
            security_name,
            financial_period_end_mm_dd,
            effective_from,
            effective_to
        from summarise
        where financial_period_end_mm_dd is not null
    ),

    get_unique_financial_period_end as (
        select company_name, financial_period_end_mm_dd,
        from filter_out_null_summary
        group by all
    ),

    check_for_unique_financial_period_end as (
        select
            company_name,
            count(financial_period_end_mm_dd) > 1 as has_multiple_financial_period_end
        from get_unique_financial_period_end
        group by all
    ),

    -- deal with one financial period end
    single_financial_period_end as (
        select
            company_name,
            financial_period_end_mm_dd,
            false as has_multiple_financial_period_end,
            min(effective_from) as effective_from,
            max(effective_to) as effective_to,
            array_agg(
                struct(
                    cast(null as string) as security_name,
                    cast(null as string) as financial_period_end_mm_dd,
                    cast(null as date) as effective_from,
                    cast(null as date) as effective_to
                )
                limit 1
            ) as additional_information
        from filter_out_null_summary
        where
            company_name in (
                select company_name
                from check_for_unique_financial_period_end
                where has_multiple_financial_period_end is false
            )
        group by all
    ),

    -- deal with multiple date ends
    multiple_financial_period_end as (
        select
            company_name,
            cast(null as string) as financial_period_end_mm_dd,
            true as has_multiple_financial_period_end,
            min(effective_from) as effective_from,
            max(effective_to) as effective_to,
            array_agg(
                struct(
                    security_name,
                    financial_period_end_mm_dd,
                    effective_from,
                    effective_to
                )
                order by effective_to desc
            ) as additional_information
        from filter_out_null_summary
        where
            company_name in (
                select company_name
                from check_for_unique_financial_period_end
                where has_multiple_financial_period_end is true
            )
        group by all
    ),

    -- fmt: off
    multiple_financial_period_end_with_latest as (
        select
            company_name,
            (
                select value.financial_period_end_mm_dd
                from unnest(additional_information) as value
                order by value.effective_to desc
                limit 1
            ) as financial_period_end_mm_dd,

            has_multiple_financial_period_end,
            effective_from,
            effective_to,
            additional_information
        from multiple_financial_period_end
    ),
    -- fmt: on

    unioned as (
        select *
        from single_financial_period_end
        union all
        select *
        from multiple_financial_period_end_with_latest
    )

select 'sg_sgx' as stock_market, *
from unioned
order by company_name
