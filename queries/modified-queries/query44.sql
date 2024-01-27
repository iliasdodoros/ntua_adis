
select  asceding.rnk, i1.i_product_name best_performing, i2.i_product_name worst_performing
from(select *
     from (select item_sk,rank() over (order by rank_col asc) rnk
           from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col 
                 from redis.store_sales.store_sales ss1
                 where ss_store_sk = 47
                 group by ss_item_sk
                 having avg(ss_net_profit) > 0.9*(select avg(ss_net_profit) rank_col
                                                  from redis.store_sales.store_sales
                                                  where ss_store_sk = 47
                                                    and ss_hdemo_sk is null
                                                  group by ss_store_sk))V1)V11
     where rnk  < 11) asceding,
    (select *
     from (select item_sk,rank() over (order by rank_col desc) rnk
           from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col
                 from redis.store_sales.store_sales ss1
                 where ss_store_sk = 47
                 group by ss_item_sk
                 having avg(ss_net_profit) > 0.9*(select avg(ss_net_profit) rank_col
                                                  from redis.store_sales.store_sales
                                                  where ss_store_sk = 47
                                                    and ss_hdemo_sk is null
                                                  group by ss_store_sk))V2)V21
     where rnk  < 11) descending,
mongodb.tpcds.item i1,
mongodb.tpcds.item i2
where asceding.rnk = descending.rnk 
  and i1.i_item_sk=asceding.item_sk
  and i2.i_item_sk=descending.item_sk
order by asceding.rnk
limit 100;


