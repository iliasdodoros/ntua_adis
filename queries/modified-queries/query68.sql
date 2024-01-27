
select  c_last_name
       ,c_first_name
       ,ca_city
       ,bought_city
       ,ss_ticket_number
       ,extended_price
       ,extended_tax
       ,list_price
 from (select ss_ticket_number
             ,ss_customer_sk
             ,ca_city bought_city
             ,sum(ss_ext_sales_price) extended_price 
             ,sum(ss_ext_list_price) list_price
             ,sum(ss_ext_tax) extended_tax 
       from redis.store_sales.store_sales
           ,cassandra.tpcds.date_dim
           ,redis.store.store
           ,redis.household_demographics.household_demographics
           ,redis.customer_address.customer_address 
       where redis.store_sales.store_sales.ss_sold_date_sk = cassandra.tpcds.date_dim.d_date_sk
         and redis.store_sales.store_sales.ss_store_sk = redis.store.store.s_store_sk  
        and redis.store_sales.store_sales.ss_hdemo_sk = redis.household_demographics.household_demographics.hd_demo_sk
        and redis.store_sales.store_sales.ss_addr_sk = redis.customer_address.customer_address.ca_address_sk
        and cassandra.tpcds.date_dim.d_dom between 1 and 2 
        and (redis.household_demographics.household_demographics.hd_dep_count = 3 or
             redis.household_demographics.household_demographics.hd_vehicle_count= 2)
        and cassandra.tpcds.date_dim.d_year in (1998,1998+1,1998+2)
        and redis.store.store.s_city in ('Oak Grove','Fairview')
       group by ss_ticket_number
               ,ss_customer_sk
               ,ss_addr_sk,ca_city) dn
      ,redis.customer.customer
      ,redis.customer_address.customer_address current_addr
 where ss_customer_sk = c_customer_sk
   and redis.customer.customer.c_current_addr_sk = current_addr.ca_address_sk
   and current_addr.ca_city <> bought_city
 order by c_last_name
         ,ss_ticket_number
 limit 100;


