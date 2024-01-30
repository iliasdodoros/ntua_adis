
select  dt.d_year
 	,redis.item.item.i_category_id
 	,redis.item.item.i_category
 	,sum(ss_ext_sales_price)
 from 	cassandra.tpcds.date_dim dt
 	,mongodb.tpcds.store_sales
 	,redis.item.item
 where dt.d_date_sk = mongodb.tpcds.store_sales.ss_sold_date_sk
 	and mongodb.tpcds.store_sales.ss_item_sk = redis.item.item.i_item_sk
 	and redis.item.item.i_manager_id = 1  	
 	and dt.d_moy=12
 	and dt.d_year=1998
 group by 	dt.d_year
 		,redis.item.item.i_category_id
 		,redis.item.item.i_category
 order by       sum(ss_ext_sales_price) desc,dt.d_year
 		,redis.item.item.i_category_id
 		,redis.item.item.i_category
limit 100 ;


