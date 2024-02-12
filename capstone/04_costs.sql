with compute 
as (
    select to_date(trunc(start_time, 'MONTH')) as month
    , sum(credits_used) as total_credits
    , sum(credits_used)*3 as total_comput_costs
    from (
    select wmh.start_time
    , wmh.end_time
    , wmh.warehouse_id
    , wmh.warehouse_name
    , wmh.credits_used
    , wmh.credits_used_compute
    , wmh.credits_used_cloud_services
    from SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY wmh
    UNION ALL
    select puh.start_time
    , puh.end_time
    , null as warehouse_id
    , 'SNOWPIPE_SERVICE' as warehouse_name
    , sum(puh.credits_used) as credits_used
    , sum(null) as credits_used_compute
    , sum(null) as credits_used_cloud_services
    from snowflake.account_usage.pipe_usage_history  puh
    group by 1, 2, 3, 4
    UNION ALL
    select ach.start_time
    , ach.end_time
    , null as warehouse_id
    , 'AUTOMATIC_CLUSTERING' as warehouse_name
    , sum(ach.credits_used) as credits_used
    , sum(null) as credits_used_compute
    , sum(null) as credits_used_cloud_services
    from snowflake.account_usage.automatic_clustering_history ach
    group by 1, 2, 3, 4
    UNION ALL
    select mvr.start_time
    , mvr.end_time
    , null as warehouse_id
    , 'MVIEW_REFRESH' as warehouse_name
    , sum(mvr.credits_used) as credits_used
    , sum(null) as credits_used_compute
    , sum(null) as credits_used_cloud_services
    from SNOWFLAKE.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY mvr
    group by 1, 2, 3, 4
    )
    where month >= '2023-01-01'
    group by month
)
, storage as (
    select date_trunc('MONTH', usage_date) as month
    , avg(total_storage_tb) as avg_storage
    ,  avg(total_storage_tb)*23 as avg_storage_cost
    from (
    select usage_date
        , round(storage_bytes/power(1024, 4), 3) as db_storage_tb
        , round(failsafe_bytes/power(1024, 4), 3) as failsafe_storage_tb
        , round(stage_bytes/power(1024, 4), 3) as stage_storage_tb
        , db_storage_tb + failsafe_storage_tb + stage_storage_tb as total_storage_tb
        , round(23 * db_storage_tb, 2) as db_storage_spend
        , round(23 * failsafe_storage_tb, 2) as failsafe_storage_spend
        , round(23 * stage_storage_tb, 2) as stage_storage_spend
        , round(23 * total_storage_tb, 2) as storage_spend
    from snowflake.account_usage.storage_usage
    )
    where month >= '2023-01-01'
    group by month
)
select c.month as month
, c.total_comput_costs as compute_costs
, s.avg_storage_cost as avg_storage_costs
, c.total_comput_costs+s.avg_storage_cost as total_monthly_cost
, sum(c.total_comput_costs+s.avg_storage_cost) over (partition by null order by c.month rows between unbounded preceding and current row) as running_total
, 400-running_total as amount_remaining
from compute c
, storage s
where c.month=s.month
order by c.month asc
;  