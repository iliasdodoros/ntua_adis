
select  count(*) 
from mongodb.tpcds.store_sales
    ,mongodb.tpcds.household_demographics 
    ,mongodb.tpcds.time_dim, mongodb.tpcds.store
where ss_sold_time_sk = mongodb.tpcds.time_dim.t_time_sk   
    and ss_hdemo_sk = mongodb.tpcds.household_demographics.hd_demo_sk 
    and ss_store_sk = s_store_sk
    and mongodb.tpcds.time_dim.t_hour = 8
    and mongodb.tpcds.time_dim.t_minute >= 30
    and mongodb.tpcds.household_demographics.hd_dep_count = 5
    and mongodb.tpcds.store.s_store_name = 'ese'
order by count(*)
limit 100;


