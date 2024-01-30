
select c_last_name
       ,c_first_name
       ,c_salutation
       ,c_preferred_cust_flag
       ,ss_ticket_number
       ,cnt from
   (select ss_ticket_number
          ,ss_customer_sk
          ,count(*) cnt
    from mongodb.tpcds.store_sales,mongodb.tpcds.date_dim,mongodb.tpcds.store,mongodb.tpcds.household_demographics
    where mongodb.tpcds.store_sales.ss_sold_date_sk = mongodb.tpcds.date_dim.d_date_sk
    and mongodb.tpcds.store_sales.ss_store_sk = mongodb.tpcds.store.s_store_sk  
    and mongodb.tpcds.store_sales.ss_hdemo_sk = mongodb.tpcds.household_demographics.hd_demo_sk
    and (mongodb.tpcds.date_dim.d_dom between 1 and 3 or mongodb.tpcds.date_dim.d_dom between 25 and 28)
    and (mongodb.tpcds.household_demographics.hd_buy_potential = '>10000' or
         mongodb.tpcds.household_demographics.hd_buy_potential = 'Unknown')
    and mongodb.tpcds.household_demographics.hd_vehicle_count > 0
    and (case when mongodb.tpcds.household_demographics.hd_vehicle_count > 0 
	then mongodb.tpcds.household_demographics.hd_dep_count/ mongodb.tpcds.household_demographics.hd_vehicle_count 
	else null 
	end)  > 1.2
    and mongodb.tpcds.date_dim.d_year in (1998,1998+1,1998+2)
    and mongodb.tpcds.store.s_county in ('Williamson County','Williamson County','Williamson County','Williamson County',
                           'Williamson County','Williamson County','Williamson County','Williamson County')
    group by ss_ticket_number,ss_customer_sk) dn,mongodb.tpcds.customer
    where ss_customer_sk = c_customer_sk
      and cnt between 15 and 20
    order by c_last_name,c_first_name,c_salutation,c_preferred_cust_flag desc, ss_ticket_number;


