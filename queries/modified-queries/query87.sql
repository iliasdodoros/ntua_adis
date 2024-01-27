
select count(*) 
from ((select distinct c_last_name, c_first_name, d_date
       from redis.store_sales.store_sales, cassandra.tpcds.date_dim, redis.customer.customer
       where redis.store_sales.store_sales.ss_sold_date_sk = cassandra.tpcds.date_dim.d_date_sk
         and redis.store_sales.store_sales.ss_customer_sk = redis.customer.customer.c_customer_sk
         and d_month_seq between 1212 and 1212+11)
       except
      (select distinct c_last_name, c_first_name, d_date
       from cassandra.tpcds.catalog_sales, cassandra.tpcds.date_dim, redis.customer.customer
       where cassandra.tpcds.catalog_sales.cs_sold_date_sk = cassandra.tpcds.date_dim.d_date_sk
         and cassandra.tpcds.catalog_sales.cs_bill_customer_sk = redis.customer.customer.c_customer_sk
         and d_month_seq between 1212 and 1212+11)
       except
      (select distinct c_last_name, c_first_name, d_date
       from mongodb.tpcds.web_sales, cassandra.tpcds.date_dim, redis.customer.customer
       where mongodb.tpcds.web_sales.ws_sold_date_sk = cassandra.tpcds.date_dim.d_date_sk
         and mongodb.tpcds.web_sales.ws_bill_customer_sk = redis.customer.customer.c_customer_sk
         and d_month_seq between 1212 and 1212+11)
) cool_cust
;


