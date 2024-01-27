
WITH web_v1 as (
select
  ws_item_sk item_sk, d_date,
  sum(sum(ws_sales_price))
      over (partition by ws_item_sk order by d_date rows between unbounded preceding and current row) cume_sales
from mongodb.tpcds.web_sales
    ,cassandra.tpcds.date_dim
where ws_sold_date_sk=d_date_sk
  and d_month_seq between 1212 and 1212+11
  and ws_item_sk is not NULL
group by ws_item_sk, d_date),
store_v1 as (
select
  ss_item_sk item_sk, d_date,
  sum(sum(ss_sales_price))
      over (partition by ss_item_sk order by d_date rows between unbounded preceding and current row) cume_sales
from redis.store_sales.store_sales
    ,cassandra.tpcds.date_dim
where ss_sold_date_sk=d_date_sk
  and d_month_seq between 1212 and 1212+11
  and ss_item_sk is not NULL
group by ss_item_sk, d_date)
 select  *
from (select item_sk
     ,d_date
     ,mongodb.tpcds.web_sales
     ,redis.store_sales.store_sales
     ,max(mongodb.tpcds.web_sales)
         over (partition by item_sk order by d_date rows between unbounded preceding and current row) web_cumulative
     ,max(redis.store_sales.store_sales)
         over (partition by item_sk order by d_date rows between unbounded preceding and current row) store_cumulative
     from (select case when web.item_sk is not null then web.item_sk else redis.store.store.item_sk end item_sk
                 ,case when web.d_date is not null then web.d_date else redis.store.store.d_date end d_date
                 ,web.cume_sales mongodb.tpcds.web_sales
                 ,redis.store.store.cume_sales redis.store_sales.store_sales
           from web_v1 web full outer join store_v1 redis.store.store on (web.item_sk = redis.store.store.item_sk
                                                          and web.d_date = redis.store.store.d_date)
          )x )y
where web_cumulative > store_cumulative
order by item_sk
        ,d_date
limit 100;


