
with my_customers as (
 select distinct c_customer_sk
        , c_current_addr_sk
 from   
        ( select cs_sold_date_sk sold_date_sk,
                 cs_bill_customer_sk customer_sk,
                 cs_item_sk item_sk
          from   cassandra.tpcds.catalog_sales
          union all
          select ws_sold_date_sk sold_date_sk,
                 ws_bill_customer_sk customer_sk,
                 ws_item_sk item_sk
          from   mongodb.tpcds.web_sales
         ) cs_or_ws_sales,
         mongodb.tpcds.item,
         cassandra.tpcds.date_dim,
         redis.customer.customer
 where   sold_date_sk = d_date_sk
         and item_sk = i_item_sk
         and i_category = 'Jewelry'
         and i_class = 'consignment'
         and c_customer_sk = cs_or_ws_sales.customer_sk
         and d_moy = 3
         and d_year = 1999
 )
 , my_revenue as (
 select c_customer_sk,
        sum(ss_ext_sales_price) as revenue
 from   my_customers,
        redis.store_sales.store_sales,
        redis.customer_address.customer_address,
        redis.store.store,
        cassandra.tpcds.date_dim
 where  c_current_addr_sk = ca_address_sk
        and ca_county = s_county
        and ca_state = s_state
        and ss_sold_date_sk = d_date_sk
        and c_customer_sk = ss_customer_sk
        and d_month_seq between (select distinct d_month_seq+1
                                 from   cassandra.tpcds.date_dim where d_year = 1999 and d_moy = 3)
                           and  (select distinct d_month_seq+3
                                 from   cassandra.tpcds.date_dim where d_year = 1999 and d_moy = 3)
 group by c_customer_sk
 )
 , segments as
 (select cast((revenue/50) as int) as segment
  from   my_revenue
 )
  select  segment, count(*) as num_customers, segment*50 as segment_base
 from segments
 group by segment
 order by segment, num_customers
 limit 100;

