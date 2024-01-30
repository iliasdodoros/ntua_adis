
select 
  c_last_name,c_first_name,substr(s_city,1,30),ss_ticket_number,amt,profit
  from
   (select ss_ticket_number
          ,ss_customer_sk
          ,mongodb.tpcds.store.s_city
          ,sum(ss_coupon_amt) amt
          ,sum(ss_net_profit) profit
    from mongodb.tpcds.store_sales,mongodb.tpcds.date_dim,mongodb.tpcds.store,mongodb.tpcds.household_demographics
    where mongodb.tpcds.store_sales.ss_sold_date_sk = mongodb.tpcds.date_dim.d_date_sk
    and mongodb.tpcds.store_sales.ss_store_sk = mongodb.tpcds.store.s_store_sk  
    and mongodb.tpcds.store_sales.ss_hdemo_sk = mongodb.tpcds.household_demographics.hd_demo_sk
    and (mongodb.tpcds.household_demographics.hd_dep_count = 8 or mongodb.tpcds.household_demographics.hd_vehicle_count > 0)
    and mongodb.tpcds.date_dim.d_dow = 1
    and mongodb.tpcds.date_dim.d_year in (1998,1998+1,1998+2) 
    and mongodb.tpcds.store.s_number_employees between 200 and 295
    group by ss_ticket_number,ss_customer_sk,ss_addr_sk,mongodb.tpcds.store.s_city) ms,mongodb.tpcds.customer
    where ss_customer_sk = c_customer_sk
 order by c_last_name,c_first_name,substr(s_city,1,30), profit
limit 100;


