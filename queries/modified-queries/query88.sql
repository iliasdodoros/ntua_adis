
select  *
from
 (select count(*) h8_30_to_9
 from redis.store_sales.store_sales, redis.household_demographics.household_demographics , cassandra.tpcds.time_dim, redis.store.store
 where ss_sold_time_sk = cassandra.tpcds.time_dim.t_time_sk   
     and ss_hdemo_sk = redis.household_demographics.household_demographics.hd_demo_sk 
     and ss_store_sk = s_store_sk
     and cassandra.tpcds.time_dim.t_hour = 8
     and cassandra.tpcds.time_dim.t_minute >= 30
     and ((redis.household_demographics.household_demographics.hd_dep_count = 3 and redis.household_demographics.household_demographics.hd_vehicle_count<=3+2) or
          (redis.household_demographics.household_demographics.hd_dep_count = 0 and redis.household_demographics.household_demographics.hd_vehicle_count<=0+2) or
          (redis.household_demographics.household_demographics.hd_dep_count = 1 and redis.household_demographics.household_demographics.hd_vehicle_count<=1+2)) 
     and redis.store.store.s_store_name = 'ese') s1,
 (select count(*) h9_to_9_30 
 from redis.store_sales.store_sales, redis.household_demographics.household_demographics , cassandra.tpcds.time_dim, redis.store.store
 where ss_sold_time_sk = cassandra.tpcds.time_dim.t_time_sk
     and ss_hdemo_sk = redis.household_demographics.household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk 
     and cassandra.tpcds.time_dim.t_hour = 9 
     and cassandra.tpcds.time_dim.t_minute < 30
     and ((redis.household_demographics.household_demographics.hd_dep_count = 3 and redis.household_demographics.household_demographics.hd_vehicle_count<=3+2) or
          (redis.household_demographics.household_demographics.hd_dep_count = 0 and redis.household_demographics.household_demographics.hd_vehicle_count<=0+2) or
          (redis.household_demographics.household_demographics.hd_dep_count = 1 and redis.household_demographics.household_demographics.hd_vehicle_count<=1+2))
     and redis.store.store.s_store_name = 'ese') s2,
 (select count(*) h9_30_to_10 
 from redis.store_sales.store_sales, redis.household_demographics.household_demographics , cassandra.tpcds.time_dim, redis.store.store
 where ss_sold_time_sk = cassandra.tpcds.time_dim.t_time_sk
     and ss_hdemo_sk = redis.household_demographics.household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and cassandra.tpcds.time_dim.t_hour = 9
     and cassandra.tpcds.time_dim.t_minute >= 30
     and ((redis.household_demographics.household_demographics.hd_dep_count = 3 and redis.household_demographics.household_demographics.hd_vehicle_count<=3+2) or
          (redis.household_demographics.household_demographics.hd_dep_count = 0 and redis.household_demographics.household_demographics.hd_vehicle_count<=0+2) or
          (redis.household_demographics.household_demographics.hd_dep_count = 1 and redis.household_demographics.household_demographics.hd_vehicle_count<=1+2))
     and redis.store.store.s_store_name = 'ese') s3,
 (select count(*) h10_to_10_30
 from redis.store_sales.store_sales, redis.household_demographics.household_demographics , cassandra.tpcds.time_dim, redis.store.store
 where ss_sold_time_sk = cassandra.tpcds.time_dim.t_time_sk
     and ss_hdemo_sk = redis.household_demographics.household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and cassandra.tpcds.time_dim.t_hour = 10 
     and cassandra.tpcds.time_dim.t_minute < 30
     and ((redis.household_demographics.household_demographics.hd_dep_count = 3 and redis.household_demographics.household_demographics.hd_vehicle_count<=3+2) or
          (redis.household_demographics.household_demographics.hd_dep_count = 0 and redis.household_demographics.household_demographics.hd_vehicle_count<=0+2) or
          (redis.household_demographics.household_demographics.hd_dep_count = 1 and redis.household_demographics.household_demographics.hd_vehicle_count<=1+2))
     and redis.store.store.s_store_name = 'ese') s4,
 (select count(*) h10_30_to_11
 from redis.store_sales.store_sales, redis.household_demographics.household_demographics , cassandra.tpcds.time_dim, redis.store.store
 where ss_sold_time_sk = cassandra.tpcds.time_dim.t_time_sk
     and ss_hdemo_sk = redis.household_demographics.household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and cassandra.tpcds.time_dim.t_hour = 10 
     and cassandra.tpcds.time_dim.t_minute >= 30
     and ((redis.household_demographics.household_demographics.hd_dep_count = 3 and redis.household_demographics.household_demographics.hd_vehicle_count<=3+2) or
          (redis.household_demographics.household_demographics.hd_dep_count = 0 and redis.household_demographics.household_demographics.hd_vehicle_count<=0+2) or
          (redis.household_demographics.household_demographics.hd_dep_count = 1 and redis.household_demographics.household_demographics.hd_vehicle_count<=1+2))
     and redis.store.store.s_store_name = 'ese') s5,
 (select count(*) h11_to_11_30
 from redis.store_sales.store_sales, redis.household_demographics.household_demographics , cassandra.tpcds.time_dim, redis.store.store
 where ss_sold_time_sk = cassandra.tpcds.time_dim.t_time_sk
     and ss_hdemo_sk = redis.household_demographics.household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk 
     and cassandra.tpcds.time_dim.t_hour = 11
     and cassandra.tpcds.time_dim.t_minute < 30
     and ((redis.household_demographics.household_demographics.hd_dep_count = 3 and redis.household_demographics.household_demographics.hd_vehicle_count<=3+2) or
          (redis.household_demographics.household_demographics.hd_dep_count = 0 and redis.household_demographics.household_demographics.hd_vehicle_count<=0+2) or
          (redis.household_demographics.household_demographics.hd_dep_count = 1 and redis.household_demographics.household_demographics.hd_vehicle_count<=1+2))
     and redis.store.store.s_store_name = 'ese') s6,
 (select count(*) h11_30_to_12
 from redis.store_sales.store_sales, redis.household_demographics.household_demographics , cassandra.tpcds.time_dim, redis.store.store
 where ss_sold_time_sk = cassandra.tpcds.time_dim.t_time_sk
     and ss_hdemo_sk = redis.household_demographics.household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and cassandra.tpcds.time_dim.t_hour = 11
     and cassandra.tpcds.time_dim.t_minute >= 30
     and ((redis.household_demographics.household_demographics.hd_dep_count = 3 and redis.household_demographics.household_demographics.hd_vehicle_count<=3+2) or
          (redis.household_demographics.household_demographics.hd_dep_count = 0 and redis.household_demographics.household_demographics.hd_vehicle_count<=0+2) or
          (redis.household_demographics.household_demographics.hd_dep_count = 1 and redis.household_demographics.household_demographics.hd_vehicle_count<=1+2))
     and redis.store.store.s_store_name = 'ese') s7,
 (select count(*) h12_to_12_30
 from redis.store_sales.store_sales, redis.household_demographics.household_demographics , cassandra.tpcds.time_dim, redis.store.store
 where ss_sold_time_sk = cassandra.tpcds.time_dim.t_time_sk
     and ss_hdemo_sk = redis.household_demographics.household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and cassandra.tpcds.time_dim.t_hour = 12
     and cassandra.tpcds.time_dim.t_minute < 30
     and ((redis.household_demographics.household_demographics.hd_dep_count = 3 and redis.household_demographics.household_demographics.hd_vehicle_count<=3+2) or
          (redis.household_demographics.household_demographics.hd_dep_count = 0 and redis.household_demographics.household_demographics.hd_vehicle_count<=0+2) or
          (redis.household_demographics.household_demographics.hd_dep_count = 1 and redis.household_demographics.household_demographics.hd_vehicle_count<=1+2))
     and redis.store.store.s_store_name = 'ese') s8
;


