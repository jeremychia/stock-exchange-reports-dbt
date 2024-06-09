with
    source as (
        select
            company_name,
            financial_period_end_mm_dd,
            has_multiple_financial_period_end,
            effective_from as data_from,
            effective_to as data_to
        from {{ ref("dim_prep_sg_sgx__companies") }}
    )

select *
from source
