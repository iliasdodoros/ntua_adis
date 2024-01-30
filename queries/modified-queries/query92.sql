
select  
   sum(ws_ext_discount_amt)  as "Excess Discount Amount" 
from 
    mongodb.tpcds.web_sales 
   ,mongodb.tpcds.item 
   ,mongodb.tpcds.date_dim
where
i_manufact_id = 269
and i_item_sk = ws_item_sk 
and d_date between '1998-03-18' and 
        (cast('1998-03-18' as date) + 90 days)
and d_date_sk = ws_sold_date_sk 
and ws_ext_discount_amt  
     > ( 
         SELECT 
            1.3 * avg(ws_ext_discount_amt) 
         FROM 
            mongodb.tpcds.web_sales 
           ,mongodb.tpcds.date_dim
         WHERE 
              ws_item_sk = i_item_sk 
          and d_date between '1998-03-18' and
                             (cast('1998-03-18' as date) + 90 days)
          and d_date_sk = ws_sold_date_sk 
      ) 
order by sum(ws_ext_discount_amt)
limit 100;


