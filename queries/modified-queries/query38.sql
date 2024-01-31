
select  count(*) from (
    select distinct c_last_name, c_first_name, d_date
    from cassandra.tpcds.store_sales, cassandra.tpcds.date_dim, cassandra.tpcds.customer
          where cassandra.tpcds.store_sales.ss_sold_date_sk = cassandra.tpcds.date_dim.d_date_sk
      and cassandra.tpcds.store_sales.ss_customer_sk = cassandra.tpcds.customer.c_customer_sk
      and d_month_seq between 1212 and 1212 + 11
  intersect
    select distinct c_last_name, c_first_name, d_date
    from cassandra.tpcds.catalog_sales, cassandra.tpcds.date_dim, cassandra.tpcds.customer
          where cassandra.tpcds.catalog_sales.cs_sold_date_sk = cassandra.tpcds.date_dim.d_date_sk
      and cassandra.tpcds.catalog_sales.cs_bill_customer_sk = cassandra.tpcds.customer.c_customer_sk
      and d_month_seq between 1212 and 1212 + 11
  intersect
    select distinct c_last_name, c_first_name, d_date
    from cassandra.tpcds.web_sales, cassandra.tpcds.date_dim, cassandra.tpcds.customer
          where cassandra.tpcds.web_sales.ws_sold_date_sk = cassandra.tpcds.date_dim.d_date_sk
      and cassandra.tpcds.web_sales.ws_bill_customer_sk = cassandra.tpcds.customer.c_customer_sk
      and d_month_seq between 1212 and 1212 + 11
) hot_cust
limit 100;


