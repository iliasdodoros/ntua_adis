
select  count(*) 
from redis.store_sales.store_sales
    ,redis.household_demographics.household_demographics 
    ,cassandra.tpcds.time_dim, redis.store.store
where ss_sold_time_sk = cassandra.tpcds.time_dim.t_time_sk   
    and ss_hdemo_sk = redis.household_demographics.household_demographics.hd_demo_sk 
    and ss_store_sk = s_store_sk
    and cassandra.tpcds.time_dim.t_hour = 8
    and cassandra.tpcds.time_dim.t_minute >= 30
    and redis.household_demographics.household_demographics.hd_dep_count = 5
    and redis.store.store.s_store_name = 'ese'
order by count(*)
limit 100;


