
select  dt.d_year 
       ,cassandra.tpcds.item.i_brand_id brand_id 
       ,cassandra.tpcds.item.i_brand brand
       ,sum(ss_ext_sales_price) sum_agg
 from  cassandra.tpcds.date_dim dt 
      ,cassandra.tpcds.store_sales
      ,cassandra.tpcds.item
 where dt.d_date_sk = cassandra.tpcds.store_sales.ss_sold_date_sk
   and cassandra.tpcds.store_sales.ss_item_sk = cassandra.tpcds.item.i_item_sk
   and cassandra.tpcds.item.i_manufact_id = 436
   and dt.d_moy=12
 group by dt.d_year
      ,cassandra.tpcds.item.i_brand
      ,cassandra.tpcds.item.i_brand_id
 order by dt.d_year
         ,sum_agg desc
         ,brand_id
 limit 100;


