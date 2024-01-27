
select c_last_name
       ,c_first_name
       ,c_salutation
       ,c_preferred_cust_flag 
       ,ss_ticket_number
       ,cnt from
   (select ss_ticket_number
          ,ss_customer_sk
          ,count(*) cnt
    from redis.store_sales.store_sales,cassandra.tpcds.date_dim,redis.store.store,redis.household_demographics.household_demographics
    where redis.store_sales.store_sales.ss_sold_date_sk = cassandra.tpcds.date_dim.d_date_sk
    and redis.store_sales.store_sales.ss_store_sk = redis.store.store.s_store_sk  
    and redis.store_sales.store_sales.ss_hdemo_sk = redis.household_demographics.household_demographics.hd_demo_sk
    and cassandra.tpcds.date_dim.d_dom between 1 and 2 
    and (redis.household_demographics.household_demographics.hd_buy_potential = '>10000' or
         redis.household_demographics.household_demographics.hd_buy_potential = 'Unknown')
    and redis.household_demographics.household_demographics.hd_vehicle_count > 0
    and case when redis.household_demographics.household_demographics.hd_vehicle_count > 0 then 
             redis.household_demographics.household_demographics.hd_dep_count/ redis.household_demographics.household_demographics.hd_vehicle_count else null end > 1
    and cassandra.tpcds.date_dim.d_year in (1999,1999+1,1999+2)
    and redis.store.store.s_county in ('Ziebach County','Williamson County','Walker County','Williamson County')
    group by ss_ticket_number,ss_customer_sk) dj,redis.customer.customer
    where ss_customer_sk = c_customer_sk
      and cnt between 1 and 5
    order by cnt desc, c_last_name asc;


