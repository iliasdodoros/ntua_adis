
select  dt.d_year 
       ,mongodb.tpcds.item.i_brand_id brand_id 
       ,mongodb.tpcds.item.i_brand brand
       ,sum(ss_ext_sales_price) sum_agg
 from  cassandra.tpcds.date_dim dt 
      ,mongodb.tpcds.store_sales
      ,mongodb.tpcds.item
 where dt.d_date_sk = mongodb.tpcds.store_sales.ss_sold_date_sk
   and mongodb.tpcds.store_sales.ss_item_sk = mongodb.tpcds.item.i_item_sk
   and mongodb.tpcds.item.i_manufact_id = 436
   and dt.d_moy=12
 group by dt.d_year
      ,mongodb.tpcds.item.i_brand
      ,mongodb.tpcds.item.i_brand_id
 order by dt.d_year
         ,sum_agg desc
         ,brand_id
 limit 100;


