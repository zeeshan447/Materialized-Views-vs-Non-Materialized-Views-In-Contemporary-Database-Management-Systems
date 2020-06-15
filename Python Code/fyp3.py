import psycopg2
from time import time
import datetime
import pyodbc
import sqlanydb
import cx_Oracle  




def postgres():
    connection = psycopg2.connect(user = "postgres",
                                password = "zeeshan",
                                host = "127.0.0.1",
                                port = "5432",
                                database = "new_tpcds")
    cursor = connection.cursor()

    ### Generating Materialized and non-materialized views
    mv_crv = """ create materialized view crv as
select d_date_sk cr_return_date_sk
      ,t_time_sk cr_return_time_sk
      ,0 CR_SHIP_DATE_SK
      ,i_item_sk cr_item_sk
      ,c1.c_customer_sk cr_refunded_customer_sk
      ,c1.c_current_cdemo_sk cr_refunded_cdemo_sk
      ,c1.c_current_hdemo_sk cr_refunded_hdemo_sk
      ,c1.c_current_addr_sk cr_refunded_addr_sk
      ,c2.c_customer_sk cr_returning_customer_sk
      ,c2.c_current_cdemo_sk cr_returning_cdemo_sk
      ,c2.c_current_hdemo_sk cr_returning_hdemo_sk
      ,c2.c_current_addr_sk cr_returing_addr_sk
      ,cc_call_center_sk cr_call_center_sk
      ,0 CR_CATALOG_PAGE_SK
      ,0 CR_SHIP_MODE_SK
      ,0 CR_WAREHOUSE_SK
      ,r_reason_sk cr_reason_sk
      ,cret_order_id cr_order_number
      ,cret_return_qty cr_return_quantity
      ,cret_return_amt cr_return_amt
      ,cret_return_tax cr_return_tax
      ,cret_return_amt + cret_return_tax as cr_return_amt_inc_tax
      ,cret_return_fee cr_fee
      ,cret_return_ship_cost cr_return_ship_cost
      ,cret_refunded_cash cr_refunded_cash
      ,cret_reversed_charge cr_reversed_charde
      ,cret_merchant_credit cr_merchant_credit
      ,cret_return_amt+cret_return_tax+cret_return_fee
         -cret_refunded_cash-cret_reversed_charge-cret_merchant_credit cr_net_loss
from s_catalog_returns left outer join date_dim on (cast(cret_return_date as date) = d_date)
                       left outer join time_dim on (( cast(substring(cret_return_time,1,2) as integer)*3600
                                                     +cast(substring(cret_return_time,4,2) as integer)*60
                                                     +cast(substring(cret_return_time,7,2) as integer)) = t_time)
                       left outer join item on (cret_item_id = i_item_id)
                       left outer join customer c1 on (cret_return_customer_id = c1.c_customer_id)
                       left outer join customer c2 on (cret_refund_customer_id = c2.c_customer_id)
                       left outer join reason on (cret_reason_id = r_reason_id)
                       left outer join call_center on (cret_call_center_id = cc_call_center_id)
where i_rec_end_date is NULL
  and cc_rec_end_date is NULL;"""




    nv_crv = """ create view ncrv as
select d_date_sk cr_return_date_sk
      ,t_time_sk cr_return_time_sk
      ,0 CR_SHIP_DATE_SK
      ,i_item_sk cr_item_sk
      ,c1.c_customer_sk cr_refunded_customer_sk
      ,c1.c_current_cdemo_sk cr_refunded_cdemo_sk
      ,c1.c_current_hdemo_sk cr_refunded_hdemo_sk
      ,c1.c_current_addr_sk cr_refunded_addr_sk
      ,c2.c_customer_sk cr_returning_customer_sk
      ,c2.c_current_cdemo_sk cr_returning_cdemo_sk
      ,c2.c_current_hdemo_sk cr_returning_hdemo_sk
      ,c2.c_current_addr_sk cr_returing_addr_sk
      ,cc_call_center_sk cr_call_center_sk
      ,0 CR_CATALOG_PAGE_SK
      ,0 CR_SHIP_MODE_SK
      ,0 CR_WAREHOUSE_SK
      ,r_reason_sk cr_reason_sk
      ,cret_order_id cr_order_number
      ,cret_return_qty cr_return_quantity
      ,cret_return_amt cr_return_amt
      ,cret_return_tax cr_return_tax
      ,cret_return_amt + cret_return_tax as cr_return_amt_inc_tax
      ,cret_return_fee cr_fee
      ,cret_return_ship_cost cr_return_ship_cost
      ,cret_refunded_cash cr_refunded_cash
      ,cret_reversed_charge cr_reversed_charde
      ,cret_merchant_credit cr_merchant_credit
      ,cret_return_amt+cret_return_tax+cret_return_fee
         -cret_refunded_cash-cret_reversed_charge-cret_merchant_credit cr_net_loss
from s_catalog_returns left outer join date_dim on (cast(cret_return_date as date) = d_date)
                       left outer join time_dim on (( cast(substring(cret_return_time,1,2) as integer)*3600
                                                     +cast(substring(cret_return_time,4,2) as integer)*60
                                                     +cast(substring(cret_return_time,7,2) as integer)) = t_time)
                       left outer join item on (cret_item_id = i_item_id)
                       left outer join customer c1 on (cret_return_customer_id = c1.c_customer_id)
                       left outer join customer c2 on (cret_refund_customer_id = c2.c_customer_id)
                       left outer join reason on (cret_reason_id = r_reason_id)
                       left outer join call_center on (cret_call_center_id = cc_call_center_id)
where i_rec_end_date is NULL
  and cc_rec_end_date is NULL;"""

    mv_csv = """ create materialized view csv as
select d1.d_date_sk cs_sold_date_sk 
      ,t_time_sk cs_sold_time_sk 
      ,d2.d_date_sk cs_ship_date_sk
      ,c1.c_customer_sk cs_bill_customer_sk
      ,c1.c_current_cdemo_sk cs_bill_cdemo_sk 
      ,c1.c_current_hdemo_sk cs_bill_hdemo_sk
      ,c1.c_current_addr_sk cs_bill_addr_sk
      ,c2.c_customer_sk cs_ship_customer_sk
      ,c2.c_current_cdemo_sk cs_ship_cdemo_sk
      ,c2.c_current_hdemo_sk cs_ship_hdemo_sk
      ,c2.c_current_addr_sk cs_ship_addr_sk
      ,cc_call_center_sk cs_call_center
      ,cp_catalog_page_sk cs_catalog_page_sk
      ,sm_ship_mode_sk cs_ship_mode_sk
      ,w_warehouse_sk cs_warehouse_sk
      ,i_item_sk cs_item_sk
      ,p_promo_sk cs_promo_sk
      ,cord_order_id cs_order_number
      ,clin_quantity cs_quantity
      ,i_wholesale_cost cs_wholesale_cost
      ,i_current_price cs_list_price
      ,clin_sales_price cs_sales_price
      ,(i_current_price-clin_sales_price)*clin_quantity cs_ext_discount_amt
      ,clin_sales_price * clin_quantity cs_ext_sales_price
      ,i_wholesale_cost * clin_quantity cs_ext_wholesale_cost 
      ,i_current_price * clin_quantity CS_EXT_LIST_PRICE
      ,i_current_price * cc_tax_percentage CS_EXT_TAX
      ,clin_coupon_amt cs_coupon_amt
      ,clin_ship_cost * clin_quantity CS_EXT_SHIP_COST
      ,(clin_sales_price * clin_quantity)-clin_coupon_amt cs_net_paid
      ,((clin_sales_price * clin_quantity)-clin_coupon_amt)*(1+cc_tax_percentage) cs_net_paid_inc_tax
      ,(clin_sales_price * clin_quantity)-clin_coupon_amt + (clin_ship_cost * clin_quantity) CS_NET_PAID_INC_SHIP
      ,(clin_sales_price * clin_quantity)-clin_coupon_amt + (clin_ship_cost * clin_quantity) 
       + i_current_price * cc_tax_percentage CS_NET_PAID_INC_SHIP_TAX
      ,((clin_sales_price * clin_quantity)-clin_coupon_amt)-(clin_quantity*i_wholesale_cost) cs_net_profit
from    s_catalog_order left outer join date_dim d1 on (cast(cord_order_date as date) = d1.d_date)
                          left outer join time_dim on (cord_order_time = t_time)
                          left outer join customer c1 on (cord_bill_customer_id = c1.c_customer_id)
                          left outer join customer c2 on (cord_ship_customer_id = c2.c_customer_id)
                          left outer join call_center on (cord_call_center_id = cc_call_center_id)
                          left outer join ship_mode on (cord_ship_mode_id = sm_ship_mode_id), 
        s_catalog_order_lineitem
                          left outer join date_dim d2 on (cast(clin_ship_date as date) = d2.d_date)
                          left outer join catalog_page on (clin_catalog_page_number = cp_catalog_page_number and
                                                           clin_catalog_number = cp_catalog_number)
                          left outer join warehouse on (clin_warehouse_id = w_warehouse_id)
                          left outer join item on (clin_item_id = i_item_id)
                          left outer join promotion on (clin_promotion_id = p_promo_id)
where   cord_order_id = clin_order_id
    and i_rec_end_date is NULL 
    and cc_rec_end_date is null;
	"""

    nv_csv = """ create view ncsv as
select d1.d_date_sk cs_sold_date_sk 
      ,t_time_sk cs_sold_time_sk 
      ,d2.d_date_sk cs_ship_date_sk
      ,c1.c_customer_sk cs_bill_customer_sk
      ,c1.c_current_cdemo_sk cs_bill_cdemo_sk 
      ,c1.c_current_hdemo_sk cs_bill_hdemo_sk
      ,c1.c_current_addr_sk cs_bill_addr_sk
      ,c2.c_customer_sk cs_ship_customer_sk
      ,c2.c_current_cdemo_sk cs_ship_cdemo_sk
      ,c2.c_current_hdemo_sk cs_ship_hdemo_sk
      ,c2.c_current_addr_sk cs_ship_addr_sk
      ,cc_call_center_sk cs_call_center
      ,cp_catalog_page_sk cs_catalog_page_sk
      ,sm_ship_mode_sk cs_ship_mode_sk
      ,w_warehouse_sk cs_warehouse_sk
      ,i_item_sk cs_item_sk
      ,p_promo_sk cs_promo_sk
      ,cord_order_id cs_order_number
      ,clin_quantity cs_quantity
      ,i_wholesale_cost cs_wholesale_cost
      ,i_current_price cs_list_price
      ,clin_sales_price cs_sales_price
      ,(i_current_price-clin_sales_price)*clin_quantity cs_ext_discount_amt
      ,clin_sales_price * clin_quantity cs_ext_sales_price
      ,i_wholesale_cost * clin_quantity cs_ext_wholesale_cost 
      ,i_current_price * clin_quantity CS_EXT_LIST_PRICE
      ,i_current_price * cc_tax_percentage CS_EXT_TAX
      ,clin_coupon_amt cs_coupon_amt
      ,clin_ship_cost * clin_quantity CS_EXT_SHIP_COST
      ,(clin_sales_price * clin_quantity)-clin_coupon_amt cs_net_paid
      ,((clin_sales_price * clin_quantity)-clin_coupon_amt)*(1+cc_tax_percentage) cs_net_paid_inc_tax
      ,(clin_sales_price * clin_quantity)-clin_coupon_amt + (clin_ship_cost * clin_quantity) CS_NET_PAID_INC_SHIP
      ,(clin_sales_price * clin_quantity)-clin_coupon_amt + (clin_ship_cost * clin_quantity) 
       + i_current_price * cc_tax_percentage CS_NET_PAID_INC_SHIP_TAX
      ,((clin_sales_price * clin_quantity)-clin_coupon_amt)-(clin_quantity*i_wholesale_cost) cs_net_profit
from    s_catalog_order left outer join date_dim d1 on (cast(cord_order_date as date) = d1.d_date)
                          left outer join time_dim on (cord_order_time = t_time)
                          left outer join customer c1 on (cord_bill_customer_id = c1.c_customer_id)
                          left outer join customer c2 on (cord_ship_customer_id = c2.c_customer_id)
                          left outer join call_center on (cord_call_center_id = cc_call_center_id)
                          left outer join ship_mode on (cord_ship_mode_id = sm_ship_mode_id), 
        s_catalog_order_lineitem
                          left outer join date_dim d2 on (cast(clin_ship_date as date) = d2.d_date)
                          left outer join catalog_page on (clin_catalog_page_number = cp_catalog_page_number and
                                                           clin_catalog_number = cp_catalog_number)
                          left outer join warehouse on (clin_warehouse_id = w_warehouse_id)
                          left outer join item on (clin_item_id = i_item_id)
                          left outer join promotion on (clin_promotion_id = p_promo_id)
where   cord_order_id = clin_order_id
    and i_rec_end_date is NULL 
    and cc_rec_end_date is null;
	"""

    mv_itemv = """ create materialized view itemv as
select  i_item_sk
      ,item_item_id i_item_id
      ,current_date i_rec_start_date
      ,cast(NULL as date) i_rec_end_date
      ,item_item_description i_item_desc
      ,item_list_price i_current_price
      ,item_wholesale_cost i_wholesalecost
      ,i_brand_id
      ,i_brand
      ,i_class_id
      ,i_class
      ,i_category_id
      ,i_category
      ,i_manufact_id
      ,i_manufact
      ,item_size i_size
      ,item_formulation i_formulation
      ,item_color i_color
      ,item_units i_units
      ,item_container i_container
      ,item_manager_id i_manager
      ,i_product_name
from s_item,
     item
where item_item_id = i_item_id
  and i_rec_end_date is null;"""

    nv_itemv = """ create view nitemv as
select  i_item_sk
      ,item_item_id i_item_id
      ,current_date i_rec_start_date
      ,cast(NULL as date) i_rec_end_date
      ,item_item_description i_item_desc
      ,item_list_price i_current_price
      ,item_wholesale_cost i_wholesalecost
      ,i_brand_id
      ,i_brand
      ,i_class_id
      ,i_class
      ,i_category_id
      ,i_category
      ,i_manufact_id
      ,i_manufact
      ,item_size i_size
      ,item_formulation i_formulation
      ,item_color i_color
      ,item_units i_units
      ,item_container i_container
      ,item_manager_id i_manager
      ,i_product_name
from s_item,
     item
where item_item_id = i_item_id
  and i_rec_end_date is null;"""

    mv_promv = """ create MATERIALIZED view promv as
select  prom_promotion_id p_promo_id
       ,d1.d_date_sk p_start_date_sk
       ,d2.d_date_sk p_end_date_sk
       ,prom_cost p_cost
       ,prom_response_target p_response_target
       ,prom_promotion_name p_promo_name
       ,prom_channel_dmail p_channel_dmail
       ,prom_channel_email p_channel_email
       ,prom_channel_catalog p_channel_catalog
       ,prom_channel_tv p_channel_tv
       ,prom_channel_radio p_channel_radio
       ,prom_channel_press p_channel_press
       ,prom_channel_event p_channel_event
       ,prom_channel_demo p_channel_demo
       ,prom_channel_details p_channel_details
       ,prom_purpose p_purpose
       ,prom_discount_active p_discount_active
from    s_promotion left outer join date_dim d1 on cast(prom_start_date as date) = d1.d_date
                    left outer join date_dim d2 on cast(prom_end_date as date) = d2.d_date;"""

    nv_promv = """create view npromv as
select  prom_promotion_id p_promo_id
       ,d1.d_date_sk p_start_date_sk
       ,d2.d_date_sk p_end_date_sk
       ,prom_cost p_cost
       ,prom_response_target p_response_target
       ,prom_promotion_name p_promo_name
       ,prom_channel_dmail p_channel_dmail
       ,prom_channel_email p_channel_email
       ,prom_channel_catalog p_channel_catalog
       ,prom_channel_tv p_channel_tv
       ,prom_channel_radio p_channel_radio
       ,prom_channel_press p_channel_press
       ,prom_channel_event p_channel_event
       ,prom_channel_demo p_channel_demo
       ,prom_channel_details p_channel_details
       ,prom_purpose p_purpose
       ,prom_discount_active p_discount_active
from    s_promotion left outer join date_dim d1 on cast(prom_start_date as date) = d1.d_date
                    left outer join date_dim d2 on cast(prom_end_date as date) = d2.d_date;"""


    mv_ccv = """create materialized  view  ccv as
select   cc_call_center_sk
        ,call_center_id cc_call_center_id
        ,current_date cc_rec_start_date
        ,cast(NULL as date) cc_rec_end_date
        ,d1.d_date_sk cc_closed_date_sk
        ,d2.d_date_sk cc_open_date_sk
        ,call_center_name cc_name
        ,call_center_class cc_class
        ,call_center_employees cc_employees
        ,call_center_sq_ft cc_sq_ft
        ,call_center_hours cc_hours
        ,call_center_manager cc_manager
        ,cc_mkt_id
        ,cc_mkt_class
        ,cc_mkt_desc
        ,cc_market_manager
        ,cc_division
        ,cc_division_name
        ,cc_company
        ,cc_company_name
        ,cc_street_number
        ,cc_street_name
        ,cc_street_type
        ,cc_suite_number
        ,cc_city
        ,cc_county
        ,cc_state
        ,cc_zip
        ,cc_country
        ,cc_gmt_offset
        ,call_center_tax_percentage cc_tax_percentage
from    s_call_center left outer join date_dim d2 on d2.d_date = cast(call_closed_date as date)
                      left outer join date_dim d1 on d1.d_date = cast(call_open_date as date),
        call_center
where  call_center_id = cc_call_center_id
   and cc_rec_end_date is null; """


    nv_ccv = """ create view  nccv as
select   cc_call_center_sk
        ,call_center_id cc_call_center_id
        ,current_date cc_rec_start_date
        ,cast(NULL as date) cc_rec_end_date
        ,d1.d_date_sk cc_closed_date_sk
        ,d2.d_date_sk cc_open_date_sk
        ,call_center_name cc_name
        ,call_center_class cc_class
        ,call_center_employees cc_employees
        ,call_center_sq_ft cc_sq_ft
        ,call_center_hours cc_hours
        ,call_center_manager cc_manager
        ,cc_mkt_id
        ,cc_mkt_class
        ,cc_mkt_desc
        ,cc_market_manager
        ,cc_division
        ,cc_division_name
        ,cc_company
        ,cc_company_name
        ,cc_street_number
        ,cc_street_name
        ,cc_street_type
        ,cc_suite_number
        ,cc_city
        ,cc_county
        ,cc_state
        ,cc_zip
        ,cc_country
        ,cc_gmt_offset
        ,call_center_tax_percentage cc_tax_percentage
from    s_call_center left outer join date_dim d2 on d2.d_date = cast(call_closed_date as date)
                      left outer join date_dim d1 on d1.d_date = cast(call_open_date as date),
        call_center
where  call_center_id = cc_call_center_id
   and cc_rec_end_date is null;"""

    mv_srv= """ create materialized view srv as
select d_date_sk sr_returned_date_sk
      ,t_time_sk sr_return_time_sk
      ,i_item_sk sr_item_sk
      ,c_customer_sk sr_customer_sk
      ,c_current_cdemo_sk sr_cdemo_sk
      ,c_current_hdemo_sk sr_hdemo_sk
      ,c_current_addr_sk sr_addr_sk
      ,s_store_sk sr_store_sk
      ,r_reason_sk sr_reason_sk
      ,sret_ticket_number sr_ticket_number
      ,sret_return_qty sr_return_quantity
      ,sret_return_amt sr_return_amt
      ,sret_return_tax sr_return_tax
      ,sret_return_amt + sret_return_tax sr_return_amt_inc_tax
      ,sret_return_fee sr_fee
      ,sret_return_ship_cost sr_return_ship_cost
      ,sret_refunded_cash sr_refunded_cash
      ,sret_reversed_charge sr_reversed_charde
      ,sret_store_credit sr_store_credit
      ,sret_return_amt+sret_return_tax+sret_return_fee
       -sret_refunded_cash-sret_reversed_charge-sret_store_credit sr_net_loss
from s_store_returns left outer join date_dim on (cast(sret_return_date as date) = d_date)
                       left outer join time_dim on (( cast(substring(sret_return_time,1,2) as integer)*3600
                                                     +cast(substring(sret_return_time,4,2) as integer)*60
                                                     +cast(substring(sret_return_time,7,2) as integer)) = t_time)
                     left outer join item on (sret_item_id = i_item_id)
                     left outer join customer on (sret_customer_id = c_customer_id)
                     left outer join store on (sret_store_id = s_store_id)
                     left outer join reason on (sret_reason_id = r_reason_id)
where i_rec_end_date is NULL
  and s_rec_end_date is NULL;"""


    nv_srv = """ create view nsrv as
select d_date_sk sr_returned_date_sk
      ,t_time_sk sr_return_time_sk
      ,i_item_sk sr_item_sk
      ,c_customer_sk sr_customer_sk
      ,c_current_cdemo_sk sr_cdemo_sk
      ,c_current_hdemo_sk sr_hdemo_sk
      ,c_current_addr_sk sr_addr_sk
      ,s_store_sk sr_store_sk
      ,r_reason_sk sr_reason_sk
      ,sret_ticket_number sr_ticket_number
      ,sret_return_qty sr_return_quantity
      ,sret_return_amt sr_return_amt
      ,sret_return_tax sr_return_tax
      ,sret_return_amt + sret_return_tax sr_return_amt_inc_tax
      ,sret_return_fee sr_fee
      ,sret_return_ship_cost sr_return_ship_cost
      ,sret_refunded_cash sr_refunded_cash
      ,sret_reversed_charge sr_reversed_charde
      ,sret_store_credit sr_store_credit
      ,sret_return_amt+sret_return_tax+sret_return_fee
       -sret_refunded_cash-sret_reversed_charge-sret_store_credit sr_net_loss
from s_store_returns left outer join date_dim on (cast(sret_return_date as date) = d_date)
                       left outer join time_dim on (( cast(substring(sret_return_time,1,2) as integer)*3600
                                                     +cast(substring(sret_return_time,4,2) as integer)*60
                                                     +cast(substring(sret_return_time,7,2) as integer)) = t_time)
                     left outer join item on (sret_item_id = i_item_id)
                     left outer join customer on (sret_customer_id = c_customer_id)
                     left outer join store on (sret_store_id = s_store_id)
                     left outer join reason on (sret_reason_id = r_reason_id)
where i_rec_end_date is NULL
  and s_rec_end_date is NULL;"""

    mv_ssv = """ create materialized view ssv as
select  d_date_sk ss_sold_date_sk, 
        t_time_sk ss_sold_time_sk, 
        i_item_sk ss_item_sk, 
        c_customer_sk ss_customer_sk, 
        c_current_cdemo_sk ss_cdemo_sk, 
        c_current_hdemo_sk ss_hdemo_sk,
        c_current_addr_sk ss_addr_sk,
        s_store_sk ss_store_sk, 
        p_promo_sk ss_promo_sk,
        purc_purchase_id ss_ticket_number, 
        plin_quantity ss_quantity, 
        i_wholesale_cost ss_wholesale_cost, 
        i_current_price ss_list_price,
        plin_sale_price ss_sales_price,
        (i_current_price-plin_sale_price)*plin_quantity ss_ext_discount_amt,
        plin_sale_price * plin_quantity ss_ext_sales_price,
        i_wholesale_cost * plin_quantity ss_ext_wholesale_cost, 
        i_current_price * plin_quantity ss_ext_list_price, 
        i_current_price * s_tax_precentage ss_ext_tax, 
        plin_coupon_amt ss_coupon_amt,
        (plin_sale_price * plin_quantity)-plin_coupon_amt ss_net_paid,
        ((plin_sale_price * plin_quantity)-plin_coupon_amt)*(1+s_tax_precentage) ss_net_paid_inc_tax,
        ((plin_sale_price * plin_quantity)-plin_coupon_amt)-(plin_quantity*i_wholesale_cost) ss_net_profit
from    s_purchase left outer join customer on (purc_customer_id = c_customer_id) 
                     left outer join store on (purc_store_id = s_store_id)
                     left outer join date_dim on (cast(purc_purchase_date as date) = d_date)
                     left outer join time_dim on (PURC_PURCHASE_TIME = t_time),
        s_purchase_lineitem left outer join promotion on plin_promotion_id = p_promo_id
                           left outer join item on plin_item_id = i_item_id
where   purc_purchase_id = plin_purchase_id
    and i_rec_end_date is NULL
    and s_rec_end_date is NULL;"""

    nv_ssv= """ create view nssv as
select  d_date_sk ss_sold_date_sk, 
        t_time_sk ss_sold_time_sk, 
        i_item_sk ss_item_sk, 
        c_customer_sk ss_customer_sk, 
        c_current_cdemo_sk ss_cdemo_sk, 
        c_current_hdemo_sk ss_hdemo_sk,
        c_current_addr_sk ss_addr_sk,
        s_store_sk ss_store_sk, 
        p_promo_sk ss_promo_sk,
        purc_purchase_id ss_ticket_number, 
        plin_quantity ss_quantity, 
        i_wholesale_cost ss_wholesale_cost, 
        i_current_price ss_list_price,
        plin_sale_price ss_sales_price,
        (i_current_price-plin_sale_price)*plin_quantity ss_ext_discount_amt,
        plin_sale_price * plin_quantity ss_ext_sales_price,
        i_wholesale_cost * plin_quantity ss_ext_wholesale_cost, 
        i_current_price * plin_quantity ss_ext_list_price, 
        i_current_price * s_tax_precentage ss_ext_tax, 
        plin_coupon_amt ss_coupon_amt,
        (plin_sale_price * plin_quantity)-plin_coupon_amt ss_net_paid,
        ((plin_sale_price * plin_quantity)-plin_coupon_amt)*(1+s_tax_precentage) ss_net_paid_inc_tax,
        ((plin_sale_price * plin_quantity)-plin_coupon_amt)-(plin_quantity*i_wholesale_cost) ss_net_profit
from    s_purchase left outer join customer on (purc_customer_id = c_customer_id) 
                     left outer join store on (purc_store_id = s_store_id)
                     left outer join date_dim on (cast(purc_purchase_date as date) = d_date)
                     left outer join time_dim on (PURC_PURCHASE_TIME = t_time),
        s_purchase_lineitem left outer join promotion on plin_promotion_id = p_promo_id
                           left outer join item on plin_item_id = i_item_id
where   purc_purchase_id = plin_purchase_id
    and i_rec_end_date is NULL
    and s_rec_end_date is NULL;"""

    mv_storv = """create MATERIALIZED view storv as
select s_store_sk
      ,stor_store_id s_store_id
      ,current_date s_rec_start_date
      ,cast(NULL as date) s_rec_end_date
      ,d1.d_date_sk s_closed_date_sk
      ,stor_name s_store_name
      ,stor_employees s_number_employees
      ,stor_floor_space s_floor_space
      ,stor_hours s_hours
      ,stor_store_manager s_manager
      ,stor_market_id s_market_id
      ,stor_geography_class s_geography_class
      ,s_market_desc
      ,stor_market_manager s_market_manager
      ,s_division_id
      ,s_division_name
      ,s_company_id
      ,s_company_name
      ,s_street_number
      ,s_street_name
      ,s_street_type
      ,s_suite_number
      ,s_city
      ,s_county
      ,s_state
      ,s_zip
      ,s_country
      ,s_gmt_offset
      ,stor_tax_percentage s_tax_percentage
from  s_store left outer join date_dim d1 on cast(stor_closed_date as date)= d1.d_date
     ,store
where  stor_store_id = s_store_id
   and s_rec_end_date is null; """

    nv_storv= """create view nstorv as
select s_store_sk
      ,stor_store_id s_store_id
      ,current_date s_rec_start_date
      ,cast(NULL as date) s_rec_end_date
      ,d1.d_date_sk s_closed_date_sk
      ,stor_name s_store_name
      ,stor_employees s_number_employees
      ,stor_floor_space s_floor_space
      ,stor_hours s_hours
      ,stor_store_manager s_manager
      ,stor_market_id s_market_id
      ,stor_geography_class s_geography_class
      ,s_market_desc
      ,stor_market_manager s_market_manager
      ,s_division_id
      ,s_division_name
      ,s_company_id
      ,s_company_name
      ,s_street_number
      ,s_street_name
      ,s_street_type
      ,s_suite_number
      ,s_city
      ,s_county
      ,s_state
      ,s_zip
      ,s_country
      ,s_gmt_offset
      ,stor_tax_percentage s_tax_percentage
from  s_store left outer join date_dim d1 on cast(stor_closed_date as date)= d1.d_date
     ,store
where  stor_store_id = s_store_id
   and s_rec_end_date is null;  """

    mv_websv = """create materialized view websv as
select  web_site_sk
      ,wsit_web_site_id web_site_id
      ,current_date web_rec_start_date
      ,cast(null as date) web_rec_end_date
      ,wsit_site_name web_name
      ,d1.d_date_sk web_open_date_sk
      ,d2.d_date_sk web_close_date_sk
      ,wsit_site_class web_class
      ,wsit_site_manager web_manager
      ,web_mkt_id 
      ,web_mkt_class
      ,web_mkt_desc
      ,web_market_manager
      ,web_company_id
      ,web_company_name
      ,web_street_number 
      ,web_street_name
      ,web_street_type 
      ,web_suite_number
      ,web_city
      ,web_county 
      ,web_state
      ,web_zip
      ,web_country 
      ,web_gmt_offset
      ,wsit_tax_percentage web_tax_percentage
from  s_web_site left outer join date_dim d1 on (d1.d_date = wsit_open_date)
                   left outer join date_dim d2 on (d2.d_date = wsit_closed_date), 
      web_site
where web_site_id = wsit_web_site_id
  and web_rec_end_date is null; """


    nv_websv = """create view nwebsv as
select  web_site_sk
      ,wsit_web_site_id web_site_id
      ,current_date web_rec_start_date
      ,cast(null as date) web_rec_end_date
      ,wsit_site_name web_name
      ,d1.d_date_sk web_open_date_sk
      ,d2.d_date_sk web_close_date_sk
      ,wsit_site_class web_class
      ,wsit_site_manager web_manager
      ,web_mkt_id 
      ,web_mkt_class
      ,web_mkt_desc
      ,web_market_manager
      ,web_company_id
      ,web_company_name
      ,web_street_number 
      ,web_street_name
      ,web_street_type 
      ,web_suite_number
      ,web_city
      ,web_county 
      ,web_state
      ,web_zip
      ,web_country 
      ,web_gmt_offset
      ,wsit_tax_percentage web_tax_percentage
from  s_web_site left outer join date_dim d1 on (d1.d_date = wsit_open_date)
                   left outer join date_dim d2 on (d2.d_date = wsit_closed_date), 
      web_site
where web_site_id = wsit_web_site_id
  and web_rec_end_date is null; """


    mv_webv = """create materialized view webv as
select wp_web_page_sk,
      wpag_web_page_id wp_web_page_id
      ,current_date wp_rec_start_date
      ,cast(null as date) wp_rec_end_date
      ,d1.d_date_sk wp_creation_date_sk
      ,d2.d_date_sk wp_access_date_sk
      ,wpag_autogen_flag wp_autogen_flag
      ,wpag_url wp_url
      ,wpag_type wp_type 
      ,wpag_char_cnt wp_char_count
      ,wpag_link_cnt wp_link_count
      ,wpag_image_cnt wp_image_count
      ,wpag_max_ad_cnt wp_max_ad_count
from   web_page, s_web_page left outer join date_dim d1 on wpag_create_date  = d1.d_date
                     left outer join date_dim d2 on wpag_access_date  = d2.d_date; """

    nv_webv = """create view nwebv as
select wp_web_page_sk,
      wpag_web_page_id wp_web_page_id
      ,current_date wp_rec_start_date
      ,cast(null as date) wp_rec_end_date
      ,d1.d_date_sk wp_creation_date_sk
      ,d2.d_date_sk wp_access_date_sk
      ,wpag_autogen_flag wp_autogen_flag
      ,wpag_url wp_url
      ,wpag_type wp_type 
      ,wpag_char_cnt wp_char_count
      ,wpag_link_cnt wp_link_count
      ,wpag_image_cnt wp_image_count
      ,wpag_max_ad_cnt wp_max_ad_count
from   web_page, s_web_page left outer join date_dim d1 on wpag_create_date  = d1.d_date
                     left outer join date_dim d2 on wpag_access_date  = d2.d_date;"""

    mv_wrhsv = """create MATERIALIZED view wrhsv as
select  wrhs_warehouse_id w_warehouse_id
       ,wrhs_warehouse_desc w_warehouse_name
       ,wrhs_warehouse_sq_ft w_warehouse_sq_ft
       ,w_street_number
       ,w_street_name
       ,w_street_type
       ,w_suite_number
       ,w_city
       ,w_county
       ,w_state
       ,w_zip
       ,w_country
       ,w_gmt_offset
from    s_warehouse,
        warehouse
where   wrhs_warehouse_id = w_warehouse_id;"""

    nv_wrhsv = """create view nwrhsv as
select  wrhs_warehouse_id w_warehouse_id
       ,wrhs_warehouse_desc w_warehouse_name
       ,wrhs_warehouse_sq_ft w_warehouse_sq_ft
       ,w_street_number
       ,w_street_name
       ,w_street_type
       ,w_suite_number
       ,w_city
       ,w_county
       ,w_state
       ,w_zip
       ,w_country
       ,w_gmt_offset
from    s_warehouse,
        warehouse
where   wrhs_warehouse_id = w_warehouse_id;"""

    mv_wrv = """create materialized view wrv as
select d_date_sk wr_return_date_sk
      ,t_time_sk wr_return_time_sk
      ,i_item_sk wr_item_sk
      ,c1.c_customer_sk wr_refunded_customer_sk
      ,c1.c_current_cdemo_sk wr_refunded_cdemo_sk
      ,c1.c_current_hdemo_sk wr_refunded_hdemo_sk
      ,c1.c_current_addr_sk wr_refunded_addr_sk
      ,c2.c_customer_sk wr_returning_customer_sk
      ,c2.c_current_cdemo_sk wr_returning_cdemo_sk
      ,c2.c_current_hdemo_sk wr_returning_hdemo_sk
      ,c2.c_current_addr_sk wr_returing_addr_sk
      ,wp_web_page_sk wr_web_page_sk 
      ,r_reason_sk wr_reason_sk
      ,wret_order_id wr_order_number
      ,wret_return_qty wr_return_quantity
      ,wret_return_amt wr_return_amt
      ,wret_return_tax wr_return_tax
      ,wret_return_amt + wret_return_tax as wr_return_amt_inc_tax
      ,wret_return_fee wr_fee
      ,wret_return_ship_cost wr_return_ship_cost
      ,wret_refunded_cash wr_refunded_cash
      ,wret_reversed_charge wr_reversed_charde
      ,wret_account_credit wr_account_credit
      ,wret_return_amt+wret_return_tax+wret_return_fee
       -wret_refunded_cash-wret_reversed_charge-wret_account_credit wr_net_loss
from s_web_returns left outer join date_dim on (cast(wret_return_date as date) = d_date)
                   left outer join time_dim on (( cast(substring(wret_return_time,1,2) as integer)*3600
                                                 +cast(substring(wret_return_time,4,2) as integer)*60
                                                 +cast(substring(wret_return_time,7,2) as integer)) = t_time)
                   left outer join item on (wret_item_id = i_item_id)
                   left outer join customer c1 on (wret_return_customer_id = c1.c_customer_id)
                   left outer join customer c2 on (wret_refund_customer_id = c2.c_customer_id)
                   left outer join reason on (wret_reason_id = r_reason_id)
                   left outer join web_page on (wret_web_site_id = WP_WEB_PAGE_id)
where i_rec_end_date is NULL
  and wp_rec_end_date is NULL;"""

    nv_wrv = """create view nwrv as
select d_date_sk wr_return_date_sk
      ,t_time_sk wr_return_time_sk
      ,i_item_sk wr_item_sk
      ,c1.c_customer_sk wr_refunded_customer_sk
      ,c1.c_current_cdemo_sk wr_refunded_cdemo_sk
      ,c1.c_current_hdemo_sk wr_refunded_hdemo_sk
      ,c1.c_current_addr_sk wr_refunded_addr_sk
      ,c2.c_customer_sk wr_returning_customer_sk
      ,c2.c_current_cdemo_sk wr_returning_cdemo_sk
      ,c2.c_current_hdemo_sk wr_returning_hdemo_sk
      ,c2.c_current_addr_sk wr_returing_addr_sk
      ,wp_web_page_sk wr_web_page_sk 
      ,r_reason_sk wr_reason_sk
      ,wret_order_id wr_order_number
      ,wret_return_qty wr_return_quantity
      ,wret_return_amt wr_return_amt
      ,wret_return_tax wr_return_tax
      ,wret_return_amt + wret_return_tax as wr_return_amt_inc_tax
      ,wret_return_fee wr_fee
      ,wret_return_ship_cost wr_return_ship_cost
      ,wret_refunded_cash wr_refunded_cash
      ,wret_reversed_charge wr_reversed_charde
      ,wret_account_credit wr_account_credit
      ,wret_return_amt+wret_return_tax+wret_return_fee
       -wret_refunded_cash-wret_reversed_charge-wret_account_credit wr_net_loss
from s_web_returns left outer join date_dim on (cast(wret_return_date as date) = d_date)
                   left outer join time_dim on (( cast(substring(wret_return_time,1,2) as integer)*3600
                                                 +cast(substring(wret_return_time,4,2) as integer)*60
                                                 +cast(substring(wret_return_time,7,2) as integer)) = t_time)
                   left outer join item on (wret_item_id = i_item_id)
                   left outer join customer c1 on (wret_return_customer_id = c1.c_customer_id)
                   left outer join customer c2 on (wret_refund_customer_id = c2.c_customer_id)
                   left outer join reason on (wret_reason_id = r_reason_id)
                   left outer join web_page on (wret_web_site_id = WP_WEB_PAGE_id)
where i_rec_end_date is NULL
  and wp_rec_end_date is NULL;"""

    mv_wsv = """create materialized view wsv as
select  d1.d_date_sk ws_sold_date_sk, 
        t_time_sk ws_sold_time_sk, 
        d2.d_date_sk ws_ship_date_sk,
        i_item_sk ws_item_sk, 
        c1.c_customer_sk ws_bill_customer_sk, 
        c1.c_current_cdemo_sk ws_bill_cdemo_sk, 
        c1.c_current_hdemo_sk ws_bill_hdemo_sk,
        c1.c_current_addr_sk ws_bill_addr_sk,
        c2.c_customer_sk ws_ship_customer_sk,
        c2.c_current_cdemo_sk ws_ship_cdemo_sk,
        c2.c_current_hdemo_sk ws_ship_hdemo_sk,
        c2.c_current_addr_sk ws_ship_addr_sk,
        web_site_sk ws_web_page_sk,
        wp_web_page_sk ws_web_site_s,
        sm_ship_mode_sk ws_ship_mode_sk,
        w_warehouse_sk ws_warehouse_sk,
        p_promo_sk ws_promo_sk,
        word_order_id ws_order_number, 
        wlin_quantity ws_quantity, 
        i_wholesale_cost ws_wholesale_cost, 
        i_current_price ws_list_price,
        wlin_sales_price ws_sales_price,
        (i_current_price-wlin_sales_price)*wlin_quantity ws_ext_discount_amt,
        wlin_sales_price * wlin_quantity ws_ext_sales_price,
        i_wholesale_cost * wlin_quantity ws_ext_wholesale_cost, 
        i_current_price * wlin_quantity ws_ext_list_price, 
        i_current_price * web_tax_percentage ws_ext_tax,  
        wlin_coupon_amt ws_coupon_amt,
        0 WS_EXT_SHIP_COST,
        (wlin_sales_price * wlin_quantity)-wlin_coupon_amt ws_net_paid,
        ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)*(1+web_tax_percentage) ws_net_paid_inc_tax,
        ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)-(wlin_quantity*i_wholesale_cost) WS_NET_PAID_INC_SHIP,
        (wlin_sales_price * wlin_quantity)-wlin_coupon_amt + (wlin_ship_cost * wlin_quantity)
        + i_current_price * web_tax_percentage WS_NET_PAID_INC_SHIP_TAX,
        ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)-(i_wholesale_cost * wlin_quantity) WS_NET_PROFIT
from    s_web_order left outer join date_dim d1 on (cast(word_order_date as date) =  d1.d_date)
                    left outer join time_dim on (word_order_time = t_time)
                    left outer join customer c1 on (word_bill_customer_id = c1.c_customer_id)
                    left outer join customer c2 on (word_ship_customer_id = c2.c_customer_id)
                    left outer join web_site on (word_web_site_id = web_site_id)
                    left outer join ship_mode on (word_ship_mode_id = sm_ship_mode_id), 
        s_web_order_lineitem left outer join date_dim d2 on (cast(wlin_ship_date as date) = d2.d_date)
                             left outer join item on (wlin_item_id = i_item_id)
                             left outer join web_page on (wlin_web_page_id = wp_web_page_id)
                             left outer join warehouse on (wlin_warehouse_id = w_warehouse_id)
                             left outer join promotion on (wlin_promotion_id = p_promo_id)
where   word_order_id = wlin_order_id
    and i_rec_end_date is NULL 
    and web_rec_end_date is null
    and wp_rec_end_date is null; """


    nv_wsv = """create view Nwsv as
select  d1.d_date_sk ws_sold_date_sk, 
        t_time_sk ws_sold_time_sk, 
        d2.d_date_sk ws_ship_date_sk,
        i_item_sk ws_item_sk, 
        c1.c_customer_sk ws_bill_customer_sk, 
        c1.c_current_cdemo_sk ws_bill_cdemo_sk, 
        c1.c_current_hdemo_sk ws_bill_hdemo_sk,
        c1.c_current_addr_sk ws_bill_addr_sk,
        c2.c_customer_sk ws_ship_customer_sk,
        c2.c_current_cdemo_sk ws_ship_cdemo_sk,
        c2.c_current_hdemo_sk ws_ship_hdemo_sk,
        c2.c_current_addr_sk ws_ship_addr_sk,
        web_site_sk ws_web_page_sk,
        wp_web_page_sk ws_web_site_s,
        sm_ship_mode_sk ws_ship_mode_sk,
        w_warehouse_sk ws_warehouse_sk,
        p_promo_sk ws_promo_sk,
        word_order_id ws_order_number, 
        wlin_quantity ws_quantity, 
        i_wholesale_cost ws_wholesale_cost, 
        i_current_price ws_list_price,
        wlin_sales_price ws_sales_price,
        (i_current_price-wlin_sales_price)*wlin_quantity ws_ext_discount_amt,
        wlin_sales_price * wlin_quantity ws_ext_sales_price,
        i_wholesale_cost * wlin_quantity ws_ext_wholesale_cost, 
        i_current_price * wlin_quantity ws_ext_list_price, 
        i_current_price * web_tax_percentage ws_ext_tax,  
        wlin_coupon_amt ws_coupon_amt,
        0 WS_EXT_SHIP_COST,
        (wlin_sales_price * wlin_quantity)-wlin_coupon_amt ws_net_paid,
        ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)*(1+web_tax_percentage) ws_net_paid_inc_tax,
        ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)-(wlin_quantity*i_wholesale_cost) WS_NET_PAID_INC_SHIP,
        (wlin_sales_price * wlin_quantity)-wlin_coupon_amt + (wlin_ship_cost * wlin_quantity)
        + i_current_price * web_tax_percentage WS_NET_PAID_INC_SHIP_TAX,
        ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)-(i_wholesale_cost * wlin_quantity) WS_NET_PROFIT
from    s_web_order left outer join date_dim d1 on (cast(word_order_date as date) =  d1.d_date)
                    left outer join time_dim on (word_order_time = t_time)
                    left outer join customer c1 on (word_bill_customer_id = c1.c_customer_id)
                    left outer join customer c2 on (word_ship_customer_id = c2.c_customer_id)
                    left outer join web_site on (word_web_site_id = web_site_id)
                    left outer join ship_mode on (word_ship_mode_id = sm_ship_mode_id), 
        s_web_order_lineitem left outer join date_dim d2 on (cast(wlin_ship_date as date) = d2.d_date)
                             left outer join item on (wlin_item_id = i_item_id)
                             left outer join web_page on (wlin_web_page_id = wp_web_page_id)
                             left outer join warehouse on (wlin_warehouse_id = w_warehouse_id)
                             left outer join promotion on (wlin_promotion_id = p_promo_id)
where   word_order_id = wlin_order_id
    and i_rec_end_date is NULL 
    and web_rec_end_date is null
    and wp_rec_end_date is null; """

    cursor.execute(mv_crv)
    cursor.execute(nv_crv)
    cursor.execute(mv_csv)
    cursor.execute(nv_csv)
    cursor.execute(mv_itemv)
    cursor.execute(nv_itemv)
    cursor.execute(mv_promv)
    cursor.execute(nv_promv)
    cursor.execute(mv_ccv)
    cursor.execute(nv_ccv)
    cursor.execute(mv_srv)
    cursor.execute(nv_srv)
    cursor.execute(mv_ssv)
    cursor.execute(nv_ssv)
    cursor.execute(mv_storv)
    cursor.execute(nv_storv)
    cursor.execute(mv_websv)
    cursor.execute(nv_websv)
    cursor.execute(mv_webv)
    cursor.execute(nv_webv)
    cursor.execute(mv_wrhsv)
    cursor.execute(nv_wrhsv)
    cursor.execute(mv_wrv)
    cursor.execute(nv_wrv)
    cursor.execute(mv_wsv)
    cursor.execute(nv_wsv)

    #Storing sql queries
    mv_sql0 = """insert into catalog_returns select * from crv;"""
    nv_sql0 = """insert into catalog_returns select * from ncrv;"""
    mv_sql1 = """insert into catalog_sales select * from csv"""
    nv_sql1 = """insert into catalog_sales select * from ncsv """
    mv_sql2 = """insert into item select * from itemv """
    nv_sql2 = """insert into item select * from nitemv """
    mv_sql3 = """select count(*) from promv s,promotion d where s.P_PROMO_ID=d.P_PROMO_ID; """
    nv_sql3 = """select count(*) from npromv s,promotion d where s.P_PROMO_ID=d.P_PROMO_ID; """
    mv_sql4 = """insert into call_center select * from ccv; """
    nv_sql4 = """insert into call_center select * from nccv; """
    mv_sql5 = """insert into store_returns select * from srv """
    nv_sql5 = """insert into store_returns select * from nsrv """
    mv_sql6 = """insert into store_sales select * from ssv """
    nv_sql6 = """insert into store_sales select * from nssv """
    mv_sql7 = """insert into store select * from storv """
    nv_sql7 = """insert into store select * from nstorv """
    mv_sql8 = """insert into web_site select * from websv """
    nv_sql8 = """insert into web_site select * from nwebsv """
    mv_sql9 = """insert into web_page select * from webv """
    nv_sql9 = """ insert into web_page select * from nwebv"""
    mv_sql10 = """select count(*) from wrhsv; """
    nv_sql10 = """select count(*) from nwrhsv; """
    mv_sql11 = """insert into web_returns select * from wrv """
    nv_sql11 = """ insert into web_returns select * from nwrv"""
    mv_sql12 = """insert into web_sales select * from wsv """
    nv_sql12 = """insert into web_sales select * from wsv """


    a = datetime.datetime.now()
    cursor.execute(mv_sql0)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized crv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql0)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized crv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql1)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized csv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql1)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized csv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql2)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized itemv ",int(delta.total_seconds()*10000))
    
    a = datetime.datetime.now()
    cursor.execute(nv_sql2)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized itemv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql3)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized promv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql3)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized promv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql4)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized ccv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql4)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized ccv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql5)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized srv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql5)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized srv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql6)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized ssv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql6)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized ssv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql7)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized storv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql7)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized storv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql8)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized websv ",int(delta.total_seconds()*10000))


    a = datetime.datetime.now()
    cursor.execute(nv_sql8)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized websv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql9)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized webv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql9)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized webv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql10)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized wrhsv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql10)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized wrhsv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql11)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized wrv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql11)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized wrv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql12)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized wsv ",int(delta.total_seconds()*10000))


    a = datetime.datetime.now()
    cursor.execute(nv_sql12)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized wsv ",int(delta.total_seconds()*10000))





  
    


    

 
    

    





    
    




def SQLServer():
    connection = pyodbc.connect('Driver={SQL Server};'
                      'Server=MUHAMMADZEE7B6B;'
                      'Database=ds;'
                      'Trusted_Connection=yes;')

    cursor = connection.cursor()
    mv_crv = """create  view crv with schemabinding as
select d_date_sk cr_return_date_sk
      ,t_time_sk cr_return_time_sk
      ,0 CR_SHIP_DATE_SK
      ,i_item_sk cr_item_sk
      ,c1.c_customer_sk cr_refunded_customer_sk
      ,c1.c_current_cdemo_sk cr_refunded_cdemo_sk
      ,c1.c_current_hdemo_sk cr_refunded_hdemo_sk
      ,c1.c_current_addr_sk cr_refunded_addr_sk
      ,c2.c_customer_sk cr_returning_customer_sk
      ,c2.c_current_cdemo_sk cr_returning_cdemo_sk
      ,c2.c_current_hdemo_sk cr_returning_hdemo_sk
      ,c2.c_current_addr_sk cr_returing_addr_sk
      ,cc_call_center_sk cr_call_center_sk
      ,0 CR_CATALOG_PAGE_SK
      ,0 CR_SHIP_MODE_SK
      ,0 CR_WAREHOUSE_SK
      ,r_reason_sk cr_reason_sk
      ,cret_order_id cr_order_number
      ,cret_return_qty cr_return_quantity
      ,cret_return_amt cr_return_amt
      ,cret_return_tax cr_return_tax
      ,cret_return_amt + cret_return_tax as cr_return_amt_inc_tax
      ,cret_return_fee cr_fee
      ,cret_return_ship_cost cr_return_ship_cost
      ,cret_refunded_cash cr_refunded_cash
      ,cret_reversed_charge cr_reversed_charde
      ,cret_merchant_credit cr_merchant_credit
      ,cret_return_amt+cret_return_tax+cret_return_fee
         -cret_refunded_cash-cret_reversed_charge-cret_merchant_credit cr_net_loss
from dbo.s_catalog_returns left outer join dbo.date_dim on (convert(date, cret_return_date) = d_date)
                       left outer join dbo.time_dim on (( cast(substring(cret_return_time,1,2) as integer)*3600
                                                     +cast(substring(cret_return_time,4,2) as integer)*60
                                                     +cast(substring(cret_return_time,7,2) as integer)) = t_time)
                       left outer join dbo.item on (cret_item_id = i_item_id)
                       left outer join dbo.customer c1 on (cret_return_customer_id = c1.c_customer_id)
                       left outer join dbo.customer c2 on (cret_refund_customer_id = c2.c_customer_id)
                       left outer join dbo.reason on (cret_reason_id = r_reason_id)
                       left outer join dbo.call_center on (cret_call_center_id = cc_call_center_id)
where i_rec_end_date is NULL
  and cc_rec_end_date is NULL;"""




    nv_crv = """ create view ncrv as
select d_date_sk cr_return_date_sk
      ,t_time_sk cr_return_time_sk
      ,0 CR_SHIP_DATE_SK
      ,i_item_sk cr_item_sk
      ,c1.c_customer_sk cr_refunded_customer_sk
      ,c1.c_current_cdemo_sk cr_refunded_cdemo_sk
      ,c1.c_current_hdemo_sk cr_refunded_hdemo_sk
      ,c1.c_current_addr_sk cr_refunded_addr_sk
      ,c2.c_customer_sk cr_returning_customer_sk
      ,c2.c_current_cdemo_sk cr_returning_cdemo_sk
      ,c2.c_current_hdemo_sk cr_returning_hdemo_sk
      ,c2.c_current_addr_sk cr_returing_addr_sk
      ,cc_call_center_sk cr_call_center_sk
      ,0 CR_CATALOG_PAGE_SK
      ,0 CR_SHIP_MODE_SK
      ,0 CR_WAREHOUSE_SK
      ,r_reason_sk cr_reason_sk
      ,cret_order_id cr_order_number
      ,cret_return_qty cr_return_quantity
      ,cret_return_amt cr_return_amt
      ,cret_return_tax cr_return_tax
      ,cret_return_amt + cret_return_tax as cr_return_amt_inc_tax
      ,cret_return_fee cr_fee
      ,cret_return_ship_cost cr_return_ship_cost
      ,cret_refunded_cash cr_refunded_cash
      ,cret_reversed_charge cr_reversed_charde
      ,cret_merchant_credit cr_merchant_credit
      ,cret_return_amt+cret_return_tax+cret_return_fee
         -cret_refunded_cash-cret_reversed_charge-cret_merchant_credit cr_net_loss
from s_catalog_returns left outer join date_dim on (cast(cret_return_date as date) = d_date)
                       left outer join time_dim on (( cast(substring(cret_return_time,1,2) as integer)*3600
                                                     +cast(substring(cret_return_time,4,2) as integer)*60
                                                     +cast(substring(cret_return_time,7,2) as integer)) = t_time)
                       left outer join item on (cret_item_id = i_item_id)
                       left outer join customer c1 on (cret_return_customer_id = c1.c_customer_id)
                       left outer join customer c2 on (cret_refund_customer_id = c2.c_customer_id)
                       left outer join reason on (cret_reason_id = r_reason_id)
                       left outer join call_center on (cret_call_center_id = cc_call_center_id)
where i_rec_end_date is NULL
  and cc_rec_end_date is NULL;"""

    mv_csv = """ create  view csv with schemabinding as
select d1.d_date_sk cs_sold_date_sk 
      ,t_time_sk cs_sold_time_sk 
      ,d2.d_date_sk cs_ship_date_sk
      ,c1.c_customer_sk cs_bill_customer_sk
      ,c1.c_current_cdemo_sk cs_bill_cdemo_sk 
      ,c1.c_current_hdemo_sk cs_bill_hdemo_sk
      ,c1.c_current_addr_sk cs_bill_addr_sk
      ,c2.c_customer_sk cs_ship_customer_sk
      ,c2.c_current_cdemo_sk cs_ship_cdemo_sk
      ,c2.c_current_hdemo_sk cs_ship_hdemo_sk
      ,c2.c_current_addr_sk cs_ship_addr_sk
      ,cc_call_center_sk cs_call_center
      ,cp_catalog_page_sk cs_catalog_page_sk
      ,sm_ship_mode_sk cs_ship_mode_sk
      ,w_warehouse_sk cs_warehouse_sk
      ,i_item_sk cs_item_sk
      ,p_promo_sk cs_promo_sk
      ,cord_order_id cs_order_number
      ,clin_quantity cs_quantity
      ,i_wholesale_cost cs_wholesale_cost
      ,i_current_price cs_list_price
      ,clin_sales_price cs_sales_price
      ,(i_current_price-clin_sales_price)*clin_quantity cs_ext_discount_amt
      ,clin_sales_price * clin_quantity cs_ext_sales_price
      ,i_wholesale_cost * clin_quantity cs_ext_wholesale_cost 
      ,i_current_price * clin_quantity CS_EXT_LIST_PRICE
      ,i_current_price * cc_tax_percentage CS_EXT_TAX
      ,clin_coupon_amt cs_coupon_amt
      ,clin_ship_cost * clin_quantity CS_EXT_SHIP_COST
      ,(clin_sales_price * clin_quantity)-clin_coupon_amt cs_net_paid
      ,((clin_sales_price * clin_quantity)-clin_coupon_amt)*(1+cc_tax_percentage) cs_net_paid_inc_tax
      ,(clin_sales_price * clin_quantity)-clin_coupon_amt + (clin_ship_cost * clin_quantity) CS_NET_PAID_INC_SHIP
      ,(clin_sales_price * clin_quantity)-clin_coupon_amt + (clin_ship_cost * clin_quantity) 
       + i_current_price * cc_tax_percentage CS_NET_PAID_INC_SHIP_TAX
      ,((clin_sales_price * clin_quantity)-clin_coupon_amt)-(clin_quantity*i_wholesale_cost) cs_net_profit
from    dbo.s_catalog_order left outer join dbo.date_dim d1 on (cast(cord_order_date as date) = d1.d_date)
                          left outer join dbo.time_dim on (cord_order_time = t_time)
                          left outer join dbo.customer c1 on (cord_bill_customer_id = c1.c_customer_id)
                          left outer join dbo.customer c2 on (cord_ship_customer_id = c2.c_customer_id)
                          left outer join dbo.call_center on (cord_call_center_id = cc_call_center_id)
                          left outer join dbo.ship_mode on (cord_ship_mode_id = sm_ship_mode_id), 
        dbo.s_catalog_order_lineitem
                          left outer join dbo.date_dim d2 on (cast(clin_ship_date as date) = d2.d_date)
                          left outer join dbo.catalog_page on (clin_catalog_page_number = cp_catalog_page_number and
                                                           clin_catalog_number = cp_catalog_number)
                          left outer join dbo.warehouse on (clin_warehouse_id = w_warehouse_id)
                          left outer join dbo.item on (clin_item_id = i_item_id)
                          left outer join dbo.promotion on (clin_promotion_id = p_promo_id)
where   cord_order_id = clin_order_id
    and i_rec_end_date is NULL 
    and cc_rec_end_date is null;
	"""

    nv_csv = """ create view ncsv as
select d1.d_date_sk cs_sold_date_sk 
      ,t_time_sk cs_sold_time_sk 
      ,d2.d_date_sk cs_ship_date_sk
      ,c1.c_customer_sk cs_bill_customer_sk
      ,c1.c_current_cdemo_sk cs_bill_cdemo_sk 
      ,c1.c_current_hdemo_sk cs_bill_hdemo_sk
      ,c1.c_current_addr_sk cs_bill_addr_sk
      ,c2.c_customer_sk cs_ship_customer_sk
      ,c2.c_current_cdemo_sk cs_ship_cdemo_sk
      ,c2.c_current_hdemo_sk cs_ship_hdemo_sk
      ,c2.c_current_addr_sk cs_ship_addr_sk
      ,cc_call_center_sk cs_call_center
      ,cp_catalog_page_sk cs_catalog_page_sk
      ,sm_ship_mode_sk cs_ship_mode_sk
      ,w_warehouse_sk cs_warehouse_sk
      ,i_item_sk cs_item_sk
      ,p_promo_sk cs_promo_sk
      ,cord_order_id cs_order_number
      ,clin_quantity cs_quantity
      ,i_wholesale_cost cs_wholesale_cost
      ,i_current_price cs_list_price
      ,clin_sales_price cs_sales_price
      ,(i_current_price-clin_sales_price)*clin_quantity cs_ext_discount_amt
      ,clin_sales_price * clin_quantity cs_ext_sales_price
      ,i_wholesale_cost * clin_quantity cs_ext_wholesale_cost 
      ,i_current_price * clin_quantity CS_EXT_LIST_PRICE
      ,i_current_price * cc_tax_percentage CS_EXT_TAX
      ,clin_coupon_amt cs_coupon_amt
      ,clin_ship_cost * clin_quantity CS_EXT_SHIP_COST
      ,(clin_sales_price * clin_quantity)-clin_coupon_amt cs_net_paid
      ,((clin_sales_price * clin_quantity)-clin_coupon_amt)*(1+cc_tax_percentage) cs_net_paid_inc_tax
      ,(clin_sales_price * clin_quantity)-clin_coupon_amt + (clin_ship_cost * clin_quantity) CS_NET_PAID_INC_SHIP
      ,(clin_sales_price * clin_quantity)-clin_coupon_amt + (clin_ship_cost * clin_quantity) 
       + i_current_price * cc_tax_percentage CS_NET_PAID_INC_SHIP_TAX
      ,((clin_sales_price * clin_quantity)-clin_coupon_amt)-(clin_quantity*i_wholesale_cost) cs_net_profit
from    s_catalog_order left outer join date_dim d1 on (cast(cord_order_date as date) = d1.d_date)
                          left outer join time_dim on (cord_order_time = t_time)
                          left outer join customer c1 on (cord_bill_customer_id = c1.c_customer_id)
                          left outer join customer c2 on (cord_ship_customer_id = c2.c_customer_id)
                          left outer join call_center on (cord_call_center_id = cc_call_center_id)
                          left outer join ship_mode on (cord_ship_mode_id = sm_ship_mode_id), 
        s_catalog_order_lineitem
                          left outer join date_dim d2 on (cast(clin_ship_date as date) = d2.d_date)
                          left outer join catalog_page on (clin_catalog_page_number = cp_catalog_page_number and
                                                           clin_catalog_number = cp_catalog_number)
                          left outer join warehouse on (clin_warehouse_id = w_warehouse_id)
                          left outer join item on (clin_item_id = i_item_id)
                          left outer join promotion on (clin_promotion_id = p_promo_id)
where   cord_order_id = clin_order_id
    and i_rec_end_date is NULL 
    and cc_rec_end_date is null;
	"""

    mv_itemv = """ create  view itemv WITH SCHEMABINDING as
select  i_item_sk
      ,item_item_id i_item_id
      ,getdate() i_rec_start_date
      ,cast(NULL as date) i_rec_end_date
      ,item_item_description i_item_desc
      ,item_list_price i_current_price
      ,item_wholesale_cost i_wholesalecost
      ,i_brand_id
      ,i_brand
      ,i_class_id
      ,i_class
      ,i_category_id
      ,i_category
      ,i_manufact_id
      ,i_manufact
      ,item_size i_size
      ,item_formulation i_formulation
      ,item_color i_color
      ,item_units i_units
      ,item_container i_container
      ,item_manager_id i_manager
      ,i_product_name
from dbo.s_item,
     dbo.item
where item_item_id = i_item_id
  and i_rec_end_date is null;
"""

    nv_itemv = """ create view nitemv as
select  i_item_sk
      ,item_item_id i_item_id
      ,current_date i_rec_start_date
      ,cast(NULL as date) i_rec_end_date
      ,item_item_description i_item_desc
      ,item_list_price i_current_price
      ,item_wholesale_cost i_wholesalecost
      ,i_brand_id
      ,i_brand
      ,i_class_id
      ,i_class
      ,i_category_id
      ,i_category
      ,i_manufact_id
      ,i_manufact
      ,item_size i_size
      ,item_formulation i_formulation
      ,item_color i_color
      ,item_units i_units
      ,item_container i_container
      ,item_manager_id i_manager
      ,i_product_name
from s_item,
     item
where item_item_id = i_item_id
  and i_rec_end_date is null;"""

    mv_promv = """ create view promv with schemabinding as
select  prom_promotion_id p_promo_id
       ,d1.d_date_sk p_start_date_sk
       ,d2.d_date_sk p_end_date_sk
       ,prom_cost p_cost
       ,prom_response_target p_response_target
       ,prom_promotion_name p_promo_name
       ,prom_channel_dmail p_channel_dmail
       ,prom_channel_email p_channel_email
       ,prom_channel_catalog p_channel_catalog
       ,prom_channel_tv p_channel_tv
       ,prom_channel_radio p_channel_radio
       ,prom_channel_press p_channel_press
       ,prom_channel_event p_channel_event
       ,prom_channel_demo p_channel_demo
       ,prom_channel_details p_channel_details
       ,prom_purpose p_purpose
       ,prom_discount_active p_discount_active
from    dbo.s_promotion left outer join dbo.date_dim d1 on cast(prom_start_date as date) = d1.d_date
                    left outer join dbo.date_dim d2 on cast(prom_end_date as date) = d2.d_date;"""

    nv_promv = """create view npromv as
select  prom_promotion_id p_promo_id
       ,d1.d_date_sk p_start_date_sk
       ,d2.d_date_sk p_end_date_sk
       ,prom_cost p_cost
       ,prom_response_target p_response_target
       ,prom_promotion_name p_promo_name
       ,prom_channel_dmail p_channel_dmail
       ,prom_channel_email p_channel_email
       ,prom_channel_catalog p_channel_catalog
       ,prom_channel_tv p_channel_tv
       ,prom_channel_radio p_channel_radio
       ,prom_channel_press p_channel_press
       ,prom_channel_event p_channel_event
       ,prom_channel_demo p_channel_demo
       ,prom_channel_details p_channel_details
       ,prom_purpose p_purpose
       ,prom_discount_active p_discount_active
from    s_promotion left outer join date_dim d1 on cast(prom_start_date as date) = d1.d_date
                    left outer join date_dim d2 on cast(prom_end_date as date) = d2.d_date;"""


    mv_ccv = """create   VIEW  CCV WITH SCHEMABINDING as
select   cc_call_center_sk
        ,call_center_id cc_call_center_id
        ,getdate() cc_rec_start_date
        ,cast(NULL as date) cc_rec_end_date
        ,d1.d_date_sk cc_closed_date_sk
        ,d2.d_date_sk cc_open_date_sk
        ,call_center_name cc_name
        ,call_center_class cc_class
        ,call_center_employees cc_employees
        ,call_center_sq_ft cc_sq_ft
        ,call_center_hours cc_hours
        ,call_center_manager cc_manager
        ,cc_mkt_id
        ,cc_mkt_class
        ,cc_mkt_desc
        ,cc_market_manager
        ,cc_division
        ,cc_division_name
        ,cc_company
        ,cc_company_name
        ,cc_street_number
        ,cc_street_name
        ,cc_street_type
        ,cc_suite_number
        ,cc_city
        ,cc_county
        ,cc_state
        ,cc_zip
        ,cc_country
        ,cc_gmt_offset
        ,call_center_tax_percentage cc_tax_percentage
from    dbo.s_call_center s left outer join dbo.date_dim d2 on d2.d_date = cast(call_closed_date as date)
                      left outer join dbo.date_dim d1 on d1.d_date = cast(call_open_date as date),
        dbo.call_center
where  call_center_id = cc_call_center_id
   and cc_rec_end_date is null;"""


    nv_ccv = """ create view  nccv as
select   cc_call_center_sk
        ,call_center_id cc_call_center_id
        ,current_date cc_rec_start_date
        ,cast(NULL as date) cc_rec_end_date
        ,d1.d_date_sk cc_closed_date_sk
        ,d2.d_date_sk cc_open_date_sk
        ,call_center_name cc_name
        ,call_center_class cc_class
        ,call_center_employees cc_employees
        ,call_center_sq_ft cc_sq_ft
        ,call_center_hours cc_hours
        ,call_center_manager cc_manager
        ,cc_mkt_id
        ,cc_mkt_class
        ,cc_mkt_desc
        ,cc_market_manager
        ,cc_division
        ,cc_division_name
        ,cc_company
        ,cc_company_name
        ,cc_street_number
        ,cc_street_name
        ,cc_street_type
        ,cc_suite_number
        ,cc_city
        ,cc_county
        ,cc_state
        ,cc_zip
        ,cc_country
        ,cc_gmt_offset
        ,call_center_tax_percentage cc_tax_percentage
from    s_call_center left outer join date_dim d2 on d2.d_date = cast(call_closed_date as date)
                      left outer join date_dim d1 on d1.d_date = cast(call_open_date as date),
        call_center
where  call_center_id = cc_call_center_id
   and cc_rec_end_date is null;"""

    mv_srv= """ create  view srv with schemabinding as
select d_date_sk sr_returned_date_sk
      ,t_time_sk sr_return_time_sk
      ,i_item_sk sr_item_sk
      ,c_customer_sk sr_customer_sk
      ,c_current_cdemo_sk sr_cdemo_sk
      ,c_current_hdemo_sk sr_hdemo_sk
      ,c_current_addr_sk sr_addr_sk
      ,s_store_sk sr_store_sk
      ,r_reason_sk sr_reason_sk
      ,sret_ticket_number sr_ticket_number
      ,sret_return_qty sr_return_quantity
      ,sret_return_amt sr_return_amt
      ,sret_return_tax sr_return_tax
      ,sret_return_amt + sret_return_tax sr_return_amt_inc_tax
      ,sret_return_fee sr_fee
      ,sret_return_ship_cost sr_return_ship_cost
      ,sret_refunded_cash sr_refunded_cash
      ,sret_reversed_charge sr_reversed_charde
      ,sret_store_credit sr_store_credit
      ,sret_return_amt+sret_return_tax+sret_return_fee
       -sret_refunded_cash-sret_reversed_charge-sret_store_credit sr_net_loss
from dbo.s_store_returns left outer join dbo.date_dim on (convert(date, sret_return_date) = d_date)
                       left outer join dbo.time_dim on (( cast(substring(sret_return_time,1,2) as integer)*3600
                                                     +cast(substring(sret_return_time,4,2) as integer)*60
                                                     +cast(substring(sret_return_time,7,2) as integer)) = t_time)
                     left outer join dbo.item on (sret_item_id = i_item_id)
                     left outer join dbo.customer on (sret_customer_id = c_customer_id)
                     left outer join dbo.store on (sret_store_id = s_store_id)
                     left outer join dbo.reason on (sret_reason_id = r_reason_id)
where i_rec_end_date is NULL
  and s_rec_end_date is NULL;"""


    nv_srv = """ create view nsrv as
select d_date_sk sr_returned_date_sk
      ,t_time_sk sr_return_time_sk
      ,i_item_sk sr_item_sk
      ,c_customer_sk sr_customer_sk
      ,c_current_cdemo_sk sr_cdemo_sk
      ,c_current_hdemo_sk sr_hdemo_sk
      ,c_current_addr_sk sr_addr_sk
      ,s_store_sk sr_store_sk
      ,r_reason_sk sr_reason_sk
      ,sret_ticket_number sr_ticket_number
      ,sret_return_qty sr_return_quantity
      ,sret_return_amt sr_return_amt
      ,sret_return_tax sr_return_tax
      ,sret_return_amt + sret_return_tax sr_return_amt_inc_tax
      ,sret_return_fee sr_fee
      ,sret_return_ship_cost sr_return_ship_cost
      ,sret_refunded_cash sr_refunded_cash
      ,sret_reversed_charge sr_reversed_charde
      ,sret_store_credit sr_store_credit
      ,sret_return_amt+sret_return_tax+sret_return_fee
       -sret_refunded_cash-sret_reversed_charge-sret_store_credit sr_net_loss
from s_store_returns left outer join date_dim on (cast(sret_return_date as date) = d_date)
                       left outer join time_dim on (( cast(substring(sret_return_time,1,2) as integer)*3600
                                                     +cast(substring(sret_return_time,4,2) as integer)*60
                                                     +cast(substring(sret_return_time,7,2) as integer)) = t_time)
                     left outer join item on (sret_item_id = i_item_id)
                     left outer join customer on (sret_customer_id = c_customer_id)
                     left outer join store on (sret_store_id = s_store_id)
                     left outer join reason on (sret_reason_id = r_reason_id)
where i_rec_end_date is NULL
  and s_rec_end_date is NULL;"""

    mv_ssv = """ create  view ssv with schemabinding as
select  d_date_sk ss_sold_date_sk, 
        t_time_sk ss_sold_time_sk, 
        i_item_sk ss_item_sk, 
        c_customer_sk ss_customer_sk, 
        c_current_cdemo_sk ss_cdemo_sk, 
        c_current_hdemo_sk ss_hdemo_sk,
        c_current_addr_sk ss_addr_sk,
        s_store_sk ss_store_sk, 
        p_promo_sk ss_promo_sk,
        purc_purchase_id ss_ticket_number, 
        plin_quantity ss_quantity, 
        i_wholesale_cost ss_wholesale_cost, 
        i_current_price ss_list_price,
        plin_sale_price ss_sales_price,
        (i_current_price-plin_sale_price)*plin_quantity ss_ext_discount_amt,
        plin_sale_price * plin_quantity ss_ext_sales_price,
        i_wholesale_cost * plin_quantity ss_ext_wholesale_cost, 
        i_current_price * plin_quantity ss_ext_list_price, 
        i_current_price * s_tax_precentage ss_ext_tax, 
        plin_coupon_amt ss_coupon_amt,
        (plin_sale_price * plin_quantity)-plin_coupon_amt ss_net_paid,
        ((plin_sale_price * plin_quantity)-plin_coupon_amt)*(1+s_tax_precentage) ss_net_paid_inc_tax,
        ((plin_sale_price * plin_quantity)-plin_coupon_amt)-(plin_quantity*i_wholesale_cost) ss_net_profit
from    dbo.s_purchase left outer join dbo.customer on (purc_customer_id = c_customer_id) 
                     left outer join dbo.store on (purc_store_id = s_store_id)
                     left outer join dbo.date_dim on (convert(date, purc_purchase_date) = d_date)
                     left outer join dbo.time_dim on (PURC_PURCHASE_TIME = t_time),
        dbo.s_purchase_lineitem left outer join dbo.promotion on plin_promotion_id = p_promo_id
                           left outer join dbo.item on plin_item_id = i_item_id
where   purc_purchase_id = plin_purchase_id
    and i_rec_end_date is NULL
    and s_rec_end_date is NULL;
	"""

    nv_ssv= """ create view nssv as
select  d_date_sk ss_sold_date_sk, 
        t_time_sk ss_sold_time_sk, 
        i_item_sk ss_item_sk, 
        c_customer_sk ss_customer_sk, 
        c_current_cdemo_sk ss_cdemo_sk, 
        c_current_hdemo_sk ss_hdemo_sk,
        c_current_addr_sk ss_addr_sk,
        s_store_sk ss_store_sk, 
        p_promo_sk ss_promo_sk,
        purc_purchase_id ss_ticket_number, 
        plin_quantity ss_quantity, 
        i_wholesale_cost ss_wholesale_cost, 
        i_current_price ss_list_price,
        plin_sale_price ss_sales_price,
        (i_current_price-plin_sale_price)*plin_quantity ss_ext_discount_amt,
        plin_sale_price * plin_quantity ss_ext_sales_price,
        i_wholesale_cost * plin_quantity ss_ext_wholesale_cost, 
        i_current_price * plin_quantity ss_ext_list_price, 
        i_current_price * s_tax_precentage ss_ext_tax, 
        plin_coupon_amt ss_coupon_amt,
        (plin_sale_price * plin_quantity)-plin_coupon_amt ss_net_paid,
        ((plin_sale_price * plin_quantity)-plin_coupon_amt)*(1+s_tax_precentage) ss_net_paid_inc_tax,
        ((plin_sale_price * plin_quantity)-plin_coupon_amt)-(plin_quantity*i_wholesale_cost) ss_net_profit
from    s_purchase left outer join customer on (purc_customer_id = c_customer_id) 
                     left outer join store on (purc_store_id = s_store_id)
                     left outer join date_dim on (cast(purc_purchase_date as date) = d_date)
                     left outer join time_dim on (PURC_PURCHASE_TIME = t_time),
        s_purchase_lineitem left outer join promotion on plin_promotion_id = p_promo_id
                           left outer join item on plin_item_id = i_item_id
where   purc_purchase_id = plin_purchase_id
    and i_rec_end_date is NULL
    and s_rec_end_date is NULL;"""

    mv_storv = """create  view storv with schemabinding as
select s_store_sk
      ,stor_store_id s_store_id
      ,getdate() s_rec_start_date
      ,cast(NULL as date) s_rec_end_date
      ,d1.d_date_sk s_closed_date_sk
      ,stor_name s_store_name
      ,stor_employees s_number_employees
      ,stor_floor_space s_floor_space
      ,stor_hours s_hours
      ,stor_store_manager s_manager
      ,stor_market_id s_market_id
      ,stor_geography_class s_geography_class
      ,s_market_desc
      ,stor_market_manager s_market_manager
      ,s_division_id
      ,s_division_name
      ,s_company_id
      ,s_company_name
      ,s_street_number
      ,s_street_name
      ,s_street_type
      ,s_suite_number
      ,s_city
      ,s_county
      ,s_state
      ,s_zip
      ,s_country
      ,s_gmt_offset
      ,stor_tax_percentage s_tax_percentage
from  dbo.s_store left outer join dbo.date_dim d1 on cast(stor_closed_date as date)= d1.d_date
     ,dbo.store
where  stor_store_id = s_store_id
   and s_rec_end_date is null; """

    nv_storv= """create view nstorv as
select s_store_sk
      ,stor_store_id s_store_id
      ,current_date s_rec_start_date
      ,cast(NULL as date) s_rec_end_date
      ,d1.d_date_sk s_closed_date_sk
      ,stor_name s_store_name
      ,stor_employees s_number_employees
      ,stor_floor_space s_floor_space
      ,stor_hours s_hours
      ,stor_store_manager s_manager
      ,stor_market_id s_market_id
      ,stor_geography_class s_geography_class
      ,s_market_desc
      ,stor_market_manager s_market_manager
      ,s_division_id
      ,s_division_name
      ,s_company_id
      ,s_company_name
      ,s_street_number
      ,s_street_name
      ,s_street_type
      ,s_suite_number
      ,s_city
      ,s_county
      ,s_state
      ,s_zip
      ,s_country
      ,s_gmt_offset
      ,stor_tax_percentage s_tax_percentage
from  s_store left outer join date_dim d1 on cast(stor_closed_date as date)= d1.d_date
     ,store
where  stor_store_id = s_store_id
   and s_rec_end_date is null;  """

    mv_websv = """create  view websv with schemabinding as
select  web_site_sk
      ,wsit_web_site_id web_site_id
      ,getdate() web_rec_start_date
      ,cast(null as date) web_rec_end_date
      ,wsit_site_name web_name
      ,d1.d_date_sk web_open_date_sk
      ,d2.d_date_sk web_close_date_sk
      ,wsit_site_class web_class
      ,wsit_site_manager web_manager
      ,web_mkt_id 
      ,web_mkt_class
      ,web_mkt_desc
      ,web_market_manager
      ,web_company_id
      ,web_company_name
      ,web_street_number 
      ,web_street_name
      ,web_street_type 
      ,web_suite_number
      ,web_city
      ,web_county 
      ,web_state
      ,web_zip
      ,web_country 
      ,web_gmt_offset
      ,wsit_tax_percentage web_tax_percentage
from  dbo.s_web_site left outer join dbo.date_dim d1 on (d1.d_date = wsit_open_date)
                   left outer join dbo.date_dim d2 on (d2.d_date = wsit_closed_date), 
      dbo.web_site
where web_site_id = wsit_web_site_id
  and web_rec_end_date is null; """


    nv_websv = """create view nwebsv as
select  web_site_sk
      ,wsit_web_site_id web_site_id
      ,current_date web_rec_start_date
      ,cast(null as date) web_rec_end_date
      ,wsit_site_name web_name
      ,d1.d_date_sk web_open_date_sk
      ,d2.d_date_sk web_close_date_sk
      ,wsit_site_class web_class
      ,wsit_site_manager web_manager
      ,web_mkt_id 
      ,web_mkt_class
      ,web_mkt_desc
      ,web_market_manager
      ,web_company_id
      ,web_company_name
      ,web_street_number 
      ,web_street_name
      ,web_street_type 
      ,web_suite_number
      ,web_city
      ,web_county 
      ,web_state
      ,web_zip
      ,web_country 
      ,web_gmt_offset
      ,wsit_tax_percentage web_tax_percentage
from  s_web_site left outer join date_dim d1 on (d1.d_date = wsit_open_date)
                   left outer join date_dim d2 on (d2.d_date = wsit_closed_date), 
      web_site
where web_site_id = wsit_web_site_id
  and web_rec_end_date is null; """


    mv_webv = """create  view webv with schemabinding as
select wp_web_page_sk,
      wpag_web_page_id wp_web_page_id
      ,getdate() wp_rec_start_date
      ,cast(null as date) wp_rec_end_date
      ,d1.d_date_sk wp_creation_date_sk
      ,d2.d_date_sk wp_access_date_sk
      ,wpag_autogen_flag wp_autogen_flag
      ,wpag_url wp_url
      ,wpag_type wp_type 
      ,wpag_char_cnt wp_char_count
      ,wpag_link_cnt wp_link_count
      ,wpag_image_cnt wp_image_count
      ,wpag_max_ad_cnt wp_max_ad_count
from   dbo.web_page, dbo.s_web_page left outer join dbo.date_dim d1 on wpag_create_date  = d1.d_date
                     left outer join dbo.date_dim d2 on wpag_access_date  = d2.d_date; """

    nv_webv = """create view nwebv as
select wp_web_page_sk,
      wpag_web_page_id wp_web_page_id
      ,current_date wp_rec_start_date
      ,cast(null as date) wp_rec_end_date
      ,d1.d_date_sk wp_creation_date_sk
      ,d2.d_date_sk wp_access_date_sk
      ,wpag_autogen_flag wp_autogen_flag
      ,wpag_url wp_url
      ,wpag_type wp_type 
      ,wpag_char_cnt wp_char_count
      ,wpag_link_cnt wp_link_count
      ,wpag_image_cnt wp_image_count
      ,wpag_max_ad_cnt wp_max_ad_count
from   web_page, s_web_page left outer join date_dim d1 on wpag_create_date  = d1.d_date
                     left outer join date_dim d2 on wpag_access_date  = d2.d_date;"""

    mv_wrhsv = """create  view wrhsv with schemabinding as
select  wrhs_warehouse_id w_warehouse_id
       ,wrhs_warehouse_desc w_warehouse_name
       ,wrhs_warehouse_sq_ft w_warehouse_sq_ft
       ,w_street_number
       ,w_street_name
       ,w_street_type
       ,w_suite_number
       ,w_city
       ,w_county
       ,w_state
       ,w_zip
       ,w_country
       ,w_gmt_offset
from    dbo.s_warehouse,
        dbo.warehouse
where   wrhs_warehouse_id = w_warehouse_id;"""

    nv_wrhsv = """create view nwrhsv as
select  wrhs_warehouse_id w_warehouse_id
       ,wrhs_warehouse_desc w_warehouse_name
       ,wrhs_warehouse_sq_ft w_warehouse_sq_ft
       ,w_street_number
       ,w_street_name
       ,w_street_type
       ,w_suite_number
       ,w_city
       ,w_county
       ,w_state
       ,w_zip
       ,w_country
       ,w_gmt_offset
from    s_warehouse,
        warehouse
where   wrhs_warehouse_id = w_warehouse_id;"""

    mv_wrv = """ create  view wrv with schemabinding as
select d_date_sk wr_return_date_sk
      ,t_time_sk wr_return_time_sk
      ,i_item_sk wr_item_sk
      ,c1.c_customer_sk wr_refunded_customer_sk
      ,c1.c_current_cdemo_sk wr_refunded_cdemo_sk
      ,c1.c_current_hdemo_sk wr_refunded_hdemo_sk
      ,c1.c_current_addr_sk wr_refunded_addr_sk
      ,c2.c_customer_sk wr_returning_customer_sk
      ,c2.c_current_cdemo_sk wr_returning_cdemo_sk
      ,c2.c_current_hdemo_sk wr_returning_hdemo_sk
      ,c2.c_current_addr_sk wr_returing_addr_sk
      ,wp_web_page_sk wr_web_page_sk 
      ,r_reason_sk wr_reason_sk
      ,wret_order_id wr_order_number
      ,wret_return_qty wr_return_quantity
      ,wret_return_amt wr_return_amt
      ,wret_return_tax wr_return_tax
      ,wret_return_amt + wret_return_tax as wr_return_amt_inc_tax
      ,wret_return_fee wr_fee
      ,wret_return_ship_cost wr_return_ship_cost
      ,wret_refunded_cash wr_refunded_cash
      ,wret_reversed_charge wr_reversed_charde
      ,wret_account_credit wr_account_credit
      ,wret_return_amt+wret_return_tax+wret_return_fee
       -wret_refunded_cash-wret_reversed_charge-wret_account_credit wr_net_loss
from dbo.s_web_returns left outer join dbo.date_dim on (cast(wret_return_date as date) = d_date)
                   left outer join dbo.time_dim on (( cast(substring(wret_return_time,1,2) as integer)*3600
                                                 +cast(substring(wret_return_time,4,2) as integer)*60
                                                 +cast(substring(wret_return_time,7,2) as integer)) = t_time)
                   left outer join dbo.item on (wret_item_id = i_item_id)
                   left outer join dbo.customer c1 on (wret_return_customer_id = c1.c_customer_id)
                   left outer join dbo.customer c2 on (wret_refund_customer_id = c2.c_customer_id)
                   left outer join dbo.reason on (wret_reason_id = r_reason_id)
                   left outer join dbo.web_page on (wret_web_site_id = WP_WEB_PAGE_id)
where i_rec_end_date is NULL
  and wp_rec_end_date is NULL;"""

    nv_wrv = """create view nwrv as
select d_date_sk wr_return_date_sk
      ,t_time_sk wr_return_time_sk
      ,i_item_sk wr_item_sk
      ,c1.c_customer_sk wr_refunded_customer_sk
      ,c1.c_current_cdemo_sk wr_refunded_cdemo_sk
      ,c1.c_current_hdemo_sk wr_refunded_hdemo_sk
      ,c1.c_current_addr_sk wr_refunded_addr_sk
      ,c2.c_customer_sk wr_returning_customer_sk
      ,c2.c_current_cdemo_sk wr_returning_cdemo_sk
      ,c2.c_current_hdemo_sk wr_returning_hdemo_sk
      ,c2.c_current_addr_sk wr_returing_addr_sk
      ,wp_web_page_sk wr_web_page_sk 
      ,r_reason_sk wr_reason_sk
      ,wret_order_id wr_order_number
      ,wret_return_qty wr_return_quantity
      ,wret_return_amt wr_return_amt
      ,wret_return_tax wr_return_tax
      ,wret_return_amt + wret_return_tax as wr_return_amt_inc_tax
      ,wret_return_fee wr_fee
      ,wret_return_ship_cost wr_return_ship_cost
      ,wret_refunded_cash wr_refunded_cash
      ,wret_reversed_charge wr_reversed_charde
      ,wret_account_credit wr_account_credit
      ,wret_return_amt+wret_return_tax+wret_return_fee
       -wret_refunded_cash-wret_reversed_charge-wret_account_credit wr_net_loss
from s_web_returns left outer join date_dim on (cast(wret_return_date as date) = d_date)
                   left outer join time_dim on (( cast(substring(wret_return_time,1,2) as integer)*3600
                                                 +cast(substring(wret_return_time,4,2) as integer)*60
                                                 +cast(substring(wret_return_time,7,2) as integer)) = t_time)
                   left outer join item on (wret_item_id = i_item_id)
                   left outer join customer c1 on (wret_return_customer_id = c1.c_customer_id)
                   left outer join customer c2 on (wret_refund_customer_id = c2.c_customer_id)
                   left outer join reason on (wret_reason_id = r_reason_id)
                   left outer join web_page on (wret_web_site_id = WP_WEB_PAGE_id)
where i_rec_end_date is NULL
  and wp_rec_end_date is NULL;"""

    mv_wsv = """create  view with schemabinding wsv as
select  d1.d_date_sk ws_sold_date_sk, 
        t_time_sk ws_sold_time_sk, 
        d2.d_date_sk ws_ship_date_sk,
        i_item_sk ws_item_sk, 
        c1.c_customer_sk ws_bill_customer_sk, 
        c1.c_current_cdemo_sk ws_bill_cdemo_sk, 
        c1.c_current_hdemo_sk ws_bill_hdemo_sk,
        c1.c_current_addr_sk ws_bill_addr_sk,
        c2.c_customer_sk ws_ship_customer_sk,
        c2.c_current_cdemo_sk ws_ship_cdemo_sk,
        c2.c_current_hdemo_sk ws_ship_hdemo_sk,
        c2.c_current_addr_sk ws_ship_addr_sk,
        web_site_sk ws_web_page_sk,
        wp_web_page_sk ws_web_site_s,
        sm_ship_mode_sk ws_ship_mode_sk,
        w_warehouse_sk ws_warehouse_sk,
        p_promo_sk ws_promo_sk,
        word_order_id ws_order_number, 
        wlin_quantity ws_quantity, 
        i_wholesale_cost ws_wholesale_cost, 
        i_current_price ws_list_price,
        wlin_sales_price ws_sales_price,
        (i_current_price-wlin_sales_price)*wlin_quantity ws_ext_discount_amt,
        wlin_sales_price * wlin_quantity ws_ext_sales_price,
        i_wholesale_cost * wlin_quantity ws_ext_wholesale_cost, 
        i_current_price * wlin_quantity ws_ext_list_price, 
        i_current_price * web_tax_percentage ws_ext_tax,  
        wlin_coupon_amt ws_coupon_amt,
        0 WS_EXT_SHIP_COST,
        (wlin_sales_price * wlin_quantity)-wlin_coupon_amt ws_net_paid,
        ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)*(1+web_tax_percentage) ws_net_paid_inc_tax,
        ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)-(wlin_quantity*i_wholesale_cost) WS_NET_PAID_INC_SHIP,
        (wlin_sales_price * wlin_quantity)-wlin_coupon_amt + (wlin_ship_cost * wlin_quantity)
        + i_current_price * web_tax_percentage WS_NET_PAID_INC_SHIP_TAX,
        ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)-(i_wholesale_cost * wlin_quantity) WS_NET_PROFIT
from    dbo.s_web_order left outer join dbo.date_dim d1 on (cast(word_order_date as date) =  d1.d_date)
                    left outer join dbo.time_dim on (word_order_time = t_time)
                    left outer join dbo.customer c1 on (word_bill_customer_id = c1.c_customer_id)
                    left outer join dbo.customer c2 on (word_ship_customer_id = c2.c_customer_id)
                    left outer join dbo.web_site on (word_web_site_id = web_site_id)
                    left outer join dbo.ship_mode on (word_ship_mode_id = sm_ship_mode_id), 
        dbo.s_web_order_lineitem left outer join date_dim d2 on (cast(wlin_ship_date as date) = d2.d_date)
                             left outer join dbo.item on (wlin_item_id = i_item_id)
                             left outer join dbo.web_page on (wlin_web_page_id = wp_web_page_id)
                             left outer join dbo.warehouse on (wlin_warehouse_id = w_warehouse_id)
                             left outer join dbo.promotion on (wlin_promotion_id = p_promo_id)
where   word_order_id = wlin_order_id
    and i_rec_end_date is NULL 
    and web_rec_end_date is null
    and wp_rec_end_date is null; """


    nv_wsv = """create view Nwsv as
select  d1.d_date_sk ws_sold_date_sk, 
        t_time_sk ws_sold_time_sk, 
        d2.d_date_sk ws_ship_date_sk,
        i_item_sk ws_item_sk, 
        c1.c_customer_sk ws_bill_customer_sk, 
        c1.c_current_cdemo_sk ws_bill_cdemo_sk, 
        c1.c_current_hdemo_sk ws_bill_hdemo_sk,
        c1.c_current_addr_sk ws_bill_addr_sk,
        c2.c_customer_sk ws_ship_customer_sk,
        c2.c_current_cdemo_sk ws_ship_cdemo_sk,
        c2.c_current_hdemo_sk ws_ship_hdemo_sk,
        c2.c_current_addr_sk ws_ship_addr_sk,
        web_site_sk ws_web_page_sk,
        wp_web_page_sk ws_web_site_s,
        sm_ship_mode_sk ws_ship_mode_sk,
        w_warehouse_sk ws_warehouse_sk,
        p_promo_sk ws_promo_sk,
        word_order_id ws_order_number, 
        wlin_quantity ws_quantity, 
        i_wholesale_cost ws_wholesale_cost, 
        i_current_price ws_list_price,
        wlin_sales_price ws_sales_price,
        (i_current_price-wlin_sales_price)*wlin_quantity ws_ext_discount_amt,
        wlin_sales_price * wlin_quantity ws_ext_sales_price,
        i_wholesale_cost * wlin_quantity ws_ext_wholesale_cost, 
        i_current_price * wlin_quantity ws_ext_list_price, 
        i_current_price * web_tax_percentage ws_ext_tax,  
        wlin_coupon_amt ws_coupon_amt,
        0 WS_EXT_SHIP_COST,
        (wlin_sales_price * wlin_quantity)-wlin_coupon_amt ws_net_paid,
        ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)*(1+web_tax_percentage) ws_net_paid_inc_tax,
        ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)-(wlin_quantity*i_wholesale_cost) WS_NET_PAID_INC_SHIP,
        (wlin_sales_price * wlin_quantity)-wlin_coupon_amt + (wlin_ship_cost * wlin_quantity)
        + i_current_price * web_tax_percentage WS_NET_PAID_INC_SHIP_TAX,
        ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)-(i_wholesale_cost * wlin_quantity) WS_NET_PROFIT
from    s_web_order left outer join date_dim d1 on (cast(word_order_date as date) =  d1.d_date)
                    left outer join time_dim on (word_order_time = t_time)
                    left outer join customer c1 on (word_bill_customer_id = c1.c_customer_id)
                    left outer join customer c2 on (word_ship_customer_id = c2.c_customer_id)
                    left outer join web_site on (word_web_site_id = web_site_id)
                    left outer join ship_mode on (word_ship_mode_id = sm_ship_mode_id), 
        s_web_order_lineitem left outer join date_dim d2 on (cast(wlin_ship_date as date) = d2.d_date)
                             left outer join item on (wlin_item_id = i_item_id)
                             left outer join web_page on (wlin_web_page_id = wp_web_page_id)
                             left outer join warehouse on (wlin_warehouse_id = w_warehouse_id)
                             left outer join promotion on (wlin_promotion_id = p_promo_id)
where   word_order_id = wlin_order_id
    and i_rec_end_date is NULL 
    and web_rec_end_date is null
    and wp_rec_end_date is null; """

    cursor.execute(mv_crv)
    cursor.execute(nv_crv)
    cursor.execute(mv_csv)
    cursor.execute(nv_csv)
    cursor.execute(mv_itemv)
    cursor.execute(nv_itemv)
    cursor.execute(mv_promv)
    cursor.execute(nv_promv)
    cursor.execute(mv_ccv)
    cursor.execute(nv_ccv)
    cursor.execute(mv_srv)
    cursor.execute(nv_srv)
    cursor.execute(mv_ssv)
    cursor.execute(nv_ssv)
    cursor.execute(mv_storv)
    cursor.execute(nv_storv)
    cursor.execute(mv_websv)
    cursor.execute(nv_websv)
    cursor.execute(mv_webv)
    cursor.execute(nv_webv)
    cursor.execute(mv_wrhsv)
    cursor.execute(nv_wrhsv)
    cursor.execute(mv_wrv)
    cursor.execute(nv_wrv)
    cursor.execute(mv_wsv)
    cursor.execute(nv_wsv)

    #Storing sql queries
    mv_sql0 = """insert into catalog_returns select * from crv;"""
    nv_sql0 = """insert into catalog_returns select * from ncrv;"""
    mv_sql1 = """insert into catalog_sales select * from csv"""
    nv_sql1 = """insert into catalog_sales select * from ncsv """
    mv_sql2 = """insert into item select * from itemv """
    nv_sql2 = """insert into item select * from nitemv """
    mv_sql3 = """select count(*) from promv s,promotion d where s.P_PROMO_ID=d.P_PROMO_ID; """
    nv_sql3 = """select count(*) from npromv s,promotion d where s.P_PROMO_ID=d.P_PROMO_ID; """
    mv_sql4 = """insert into call_center select * from ccv; """
    nv_sql4 = """insert into call_center select * from nccv; """
    mv_sql5 = """insert into store_returns select * from srv """
    nv_sql5 = """insert into store_returns select * from nsrv """
    mv_sql6 = """insert into store_sales select * from ssv """
    nv_sql6 = """insert into store_sales select * from nssv """
    mv_sql7 = """insert into store select * from storv """
    nv_sql7 = """insert into store select * from nstorv """
    mv_sql8 = """insert into web_site select * from websv """
    nv_sql8 = """insert into web_site select * from nwebsv """
    mv_sql9 = """insert into web_page select * from webv """
    nv_sql9 = """ insert into web_page select * from nwebv"""
    mv_sql10 = """select count(*) from wrhsv; """
    nv_sql10 = """select count(*) from nwrhsv; """
    mv_sql11 = """insert into web_returns select * from wrv """
    nv_sql11 = """ insert into web_returns select * from nwrv"""
    mv_sql12 = """insert into web_sales select * from wsv """
    nv_sql12 = """insert into web_sales select * from wsv """


    a = datetime.datetime.now()
    cursor.execute(mv_sql0)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized crv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql0)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized crv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql1)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized csv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql1)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized csv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql2)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized itemv ",int(delta.total_seconds()*10000))
    
    a = datetime.datetime.now()
    cursor.execute(nv_sql2)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized itemv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql3)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized promv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql3)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized promv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql4)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized ccv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql4)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized ccv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql5)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized srv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql5)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized srv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql6)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized ssv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql6)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized ssv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql7)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized storv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql7)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized storv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql8)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized websv ",int(delta.total_seconds()*10000))


    a = datetime.datetime.now()
    cursor.execute(nv_sql8)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized websv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql9)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized webv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql9)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized webv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql10)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized wrhsv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql10)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized wrhsv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql11)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized wrv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql11)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized wrv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql12)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized wsv ",int(delta.total_seconds()*10000))


    a = datetime.datetime.now()
    cursor.execute(nv_sql12)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized wsv ",int(delta.total_seconds()*10000))


def SQLAnywhere():



    connection = sqlanydb.connect(uid='zeeshan', pwd='zeeshan', dbn='tpcds') 
    cursor = connection.cursor()

    mv_crv = """ create materialized view crv as
select d_date_sk cr_return_date_sk
      ,t_time_sk cr_return_time_sk
      ,0 CR_SHIP_DATE_SK
      ,i_item_sk cr_item_sk
      ,c1.c_customer_sk cr_refunded_customer_sk
      ,c1.c_current_cdemo_sk cr_refunded_cdemo_sk
      ,c1.c_current_hdemo_sk cr_refunded_hdemo_sk
      ,c1.c_current_addr_sk cr_refunded_addr_sk
      ,c2.c_customer_sk cr_returning_customer_sk
      ,c2.c_current_cdemo_sk cr_returning_cdemo_sk
      ,c2.c_current_hdemo_sk cr_returning_hdemo_sk
      ,c2.c_current_addr_sk cr_returing_addr_sk
      ,cc_call_center_sk cr_call_center_sk
      ,0 CR_CATALOG_PAGE_SK
      ,0 CR_SHIP_MODE_SK
      ,0 CR_WAREHOUSE_SK
      ,r_reason_sk cr_reason_sk
      ,cret_order_id cr_order_number
      ,cret_return_qty cr_return_quantity
      ,cret_return_amt cr_return_amt
      ,cret_return_tax cr_return_tax
      ,cret_return_amt + cret_return_tax as cr_return_amt_inc_tax
      ,cret_return_fee cr_fee
      ,cret_return_ship_cost cr_return_ship_cost
      ,cret_refunded_cash cr_refunded_cash
      ,cret_reversed_charge cr_reversed_charde
      ,cret_merchant_credit cr_merchant_credit
      ,cret_return_amt+cret_return_tax+cret_return_fee
         -cret_refunded_cash-cret_reversed_charge-cret_merchant_credit cr_net_loss
from s_catalog_returns left outer join date_dim on (cast(cret_return_date as date) = d_date)
                       left outer join time_dim on (( cast(substring(cret_return_time,1,2) as integer)*3600
                                                     +cast(substring(cret_return_time,4,2) as integer)*60
                                                     +cast(substring(cret_return_time,7,2) as integer)) = t_time)
                       left outer join item on (cret_item_id = i_item_id)
                       left outer join customer c1 on (cret_return_customer_id = c1.c_customer_id)
                       left outer join customer c2 on (cret_refund_customer_id = c2.c_customer_id)
                       left outer join reason on (cret_reason_id = r_reason_id)
                       left outer join call_center on (cret_call_center_id = cc_call_center_id)
where i_rec_end_date is NULL
  and cc_rec_end_date is NULL REFRESH MATERIALIZED VIEW crv;"""




    nv_crv = """ create view ncrv as
select d_date_sk cr_return_date_sk
      ,t_time_sk cr_return_time_sk
      ,0 CR_SHIP_DATE_SK
      ,i_item_sk cr_item_sk
      ,c1.c_customer_sk cr_refunded_customer_sk
      ,c1.c_current_cdemo_sk cr_refunded_cdemo_sk
      ,c1.c_current_hdemo_sk cr_refunded_hdemo_sk
      ,c1.c_current_addr_sk cr_refunded_addr_sk
      ,c2.c_customer_sk cr_returning_customer_sk
      ,c2.c_current_cdemo_sk cr_returning_cdemo_sk
      ,c2.c_current_hdemo_sk cr_returning_hdemo_sk
      ,c2.c_current_addr_sk cr_returing_addr_sk
      ,cc_call_center_sk cr_call_center_sk
      ,0 CR_CATALOG_PAGE_SK
      ,0 CR_SHIP_MODE_SK
      ,0 CR_WAREHOUSE_SK
      ,r_reason_sk cr_reason_sk
      ,cret_order_id cr_order_number
      ,cret_return_qty cr_return_quantity
      ,cret_return_amt cr_return_amt
      ,cret_return_tax cr_return_tax
      ,cret_return_amt + cret_return_tax as cr_return_amt_inc_tax
      ,cret_return_fee cr_fee
      ,cret_return_ship_cost cr_return_ship_cost
      ,cret_refunded_cash cr_refunded_cash
      ,cret_reversed_charge cr_reversed_charde
      ,cret_merchant_credit cr_merchant_credit
      ,cret_return_amt+cret_return_tax+cret_return_fee
         -cret_refunded_cash-cret_reversed_charge-cret_merchant_credit cr_net_loss
from s_catalog_returns left outer join date_dim on (cast(cret_return_date as date) = d_date)
                       left outer join time_dim on (( cast(substring(cret_return_time,1,2) as integer)*3600
                                                     +cast(substring(cret_return_time,4,2) as integer)*60
                                                     +cast(substring(cret_return_time,7,2) as integer)) = t_time)
                       left outer join item on (cret_item_id = i_item_id)
                       left outer join customer c1 on (cret_return_customer_id = c1.c_customer_id)
                       left outer join customer c2 on (cret_refund_customer_id = c2.c_customer_id)
                       left outer join reason on (cret_reason_id = r_reason_id)
                       left outer join call_center on (cret_call_center_id = cc_call_center_id)
where i_rec_end_date is NULL
  and cc_rec_end_date is NULL;"""

    mv_csv = """ create materialized view csv as
select d1.d_date_sk cs_sold_date_sk 
      ,t_time_sk cs_sold_time_sk 
      ,d2.d_date_sk cs_ship_date_sk
      ,c1.c_customer_sk cs_bill_customer_sk
      ,c1.c_current_cdemo_sk cs_bill_cdemo_sk 
      ,c1.c_current_hdemo_sk cs_bill_hdemo_sk
      ,c1.c_current_addr_sk cs_bill_addr_sk
      ,c2.c_customer_sk cs_ship_customer_sk
      ,c2.c_current_cdemo_sk cs_ship_cdemo_sk
      ,c2.c_current_hdemo_sk cs_ship_hdemo_sk
      ,c2.c_current_addr_sk cs_ship_addr_sk
      ,cc_call_center_sk cs_call_center
      ,cp_catalog_page_sk cs_catalog_page_sk
      ,sm_ship_mode_sk cs_ship_mode_sk
      ,w_warehouse_sk cs_warehouse_sk
      ,i_item_sk cs_item_sk
      ,p_promo_sk cs_promo_sk
      ,cord_order_id cs_order_number
      ,clin_quantity cs_quantity
      ,i_wholesale_cost cs_wholesale_cost
      ,i_current_price cs_list_price
      ,clin_sales_price cs_sales_price
      ,(i_current_price-clin_sales_price)*clin_quantity cs_ext_discount_amt
      ,clin_sales_price * clin_quantity cs_ext_sales_price
      ,i_wholesale_cost * clin_quantity cs_ext_wholesale_cost 
      ,i_current_price * clin_quantity CS_EXT_LIST_PRICE
      ,i_current_price * cc_tax_percentage CS_EXT_TAX
      ,clin_coupon_amt cs_coupon_amt
      ,clin_ship_cost * clin_quantity CS_EXT_SHIP_COST
      ,(clin_sales_price * clin_quantity)-clin_coupon_amt cs_net_paid
      ,((clin_sales_price * clin_quantity)-clin_coupon_amt)*(1+cc_tax_percentage) cs_net_paid_inc_tax
      ,(clin_sales_price * clin_quantity)-clin_coupon_amt + (clin_ship_cost * clin_quantity) CS_NET_PAID_INC_SHIP
      ,(clin_sales_price * clin_quantity)-clin_coupon_amt + (clin_ship_cost * clin_quantity) 
       + i_current_price * cc_tax_percentage CS_NET_PAID_INC_SHIP_TAX
      ,((clin_sales_price * clin_quantity)-clin_coupon_amt)-(clin_quantity*i_wholesale_cost) cs_net_profit
from    s_catalog_order left outer join date_dim d1 on (cast(cord_order_date as date) = d1.d_date)
                          left outer join time_dim on (cord_order_time = t_time)
                          left outer join customer c1 on (cord_bill_customer_id = c1.c_customer_id)
                          left outer join customer c2 on (cord_ship_customer_id = c2.c_customer_id)
                          left outer join call_center on (cord_call_center_id = cc_call_center_id)
                          left outer join ship_mode on (cord_ship_mode_id = sm_ship_mode_id), 
        s_catalog_order_lineitem
                          left outer join date_dim d2 on (cast(clin_ship_date as date) = d2.d_date)
                          left outer join catalog_page on (clin_catalog_page_number = cp_catalog_page_number and
                                                           clin_catalog_number = cp_catalog_number)
                          left outer join warehouse on (clin_warehouse_id = w_warehouse_id)
                          left outer join item on (clin_item_id = i_item_id)
                          left outer join promotion on (clin_promotion_id = p_promo_id)
where   cord_order_id = clin_order_id
    and i_rec_end_date is NULL 
    and cc_rec_end_date is null REFRESH MATERIALIZED VIEW csv;
	"""

    nv_csv = """ create view ncsv as
select d1.d_date_sk cs_sold_date_sk 
      ,t_time_sk cs_sold_time_sk 
      ,d2.d_date_sk cs_ship_date_sk
      ,c1.c_customer_sk cs_bill_customer_sk
      ,c1.c_current_cdemo_sk cs_bill_cdemo_sk 
      ,c1.c_current_hdemo_sk cs_bill_hdemo_sk
      ,c1.c_current_addr_sk cs_bill_addr_sk
      ,c2.c_customer_sk cs_ship_customer_sk
      ,c2.c_current_cdemo_sk cs_ship_cdemo_sk
      ,c2.c_current_hdemo_sk cs_ship_hdemo_sk
      ,c2.c_current_addr_sk cs_ship_addr_sk
      ,cc_call_center_sk cs_call_center
      ,cp_catalog_page_sk cs_catalog_page_sk
      ,sm_ship_mode_sk cs_ship_mode_sk
      ,w_warehouse_sk cs_warehouse_sk
      ,i_item_sk cs_item_sk
      ,p_promo_sk cs_promo_sk
      ,cord_order_id cs_order_number
      ,clin_quantity cs_quantity
      ,i_wholesale_cost cs_wholesale_cost
      ,i_current_price cs_list_price
      ,clin_sales_price cs_sales_price
      ,(i_current_price-clin_sales_price)*clin_quantity cs_ext_discount_amt
      ,clin_sales_price * clin_quantity cs_ext_sales_price
      ,i_wholesale_cost * clin_quantity cs_ext_wholesale_cost 
      ,i_current_price * clin_quantity CS_EXT_LIST_PRICE
      ,i_current_price * cc_tax_percentage CS_EXT_TAX
      ,clin_coupon_amt cs_coupon_amt
      ,clin_ship_cost * clin_quantity CS_EXT_SHIP_COST
      ,(clin_sales_price * clin_quantity)-clin_coupon_amt cs_net_paid
      ,((clin_sales_price * clin_quantity)-clin_coupon_amt)*(1+cc_tax_percentage) cs_net_paid_inc_tax
      ,(clin_sales_price * clin_quantity)-clin_coupon_amt + (clin_ship_cost * clin_quantity) CS_NET_PAID_INC_SHIP
      ,(clin_sales_price * clin_quantity)-clin_coupon_amt + (clin_ship_cost * clin_quantity) 
       + i_current_price * cc_tax_percentage CS_NET_PAID_INC_SHIP_TAX
      ,((clin_sales_price * clin_quantity)-clin_coupon_amt)-(clin_quantity*i_wholesale_cost) cs_net_profit
from    s_catalog_order left outer join date_dim d1 on (cast(cord_order_date as date) = d1.d_date)
                          left outer join time_dim on (cord_order_time = t_time)
                          left outer join customer c1 on (cord_bill_customer_id = c1.c_customer_id)
                          left outer join customer c2 on (cord_ship_customer_id = c2.c_customer_id)
                          left outer join call_center on (cord_call_center_id = cc_call_center_id)
                          left outer join ship_mode on (cord_ship_mode_id = sm_ship_mode_id), 
        s_catalog_order_lineitem
                          left outer join date_dim d2 on (cast(clin_ship_date as date) = d2.d_date)
                          left outer join catalog_page on (clin_catalog_page_number = cp_catalog_page_number and
                                                           clin_catalog_number = cp_catalog_number)
                          left outer join warehouse on (clin_warehouse_id = w_warehouse_id)
                          left outer join item on (clin_item_id = i_item_id)
                          left outer join promotion on (clin_promotion_id = p_promo_id)
where   cord_order_id = clin_order_id
    and i_rec_end_date is NULL 
    and cc_rec_end_date is null;
	"""

    mv_itemv = """ create materialized view itemv as
select  i_item_sk
      ,item_item_id i_item_id
      ,current_date i_rec_start_date
      ,cast(NULL as date) i_rec_end_date
      ,item_item_description i_item_desc
      ,item_list_price i_current_price
      ,item_wholesale_cost i_wholesalecost
      ,i_brand_id
      ,i_brand
      ,i_class_id
      ,i_class
      ,i_category_id
      ,i_category
      ,i_manufact_id
      ,i_manufact
      ,item_size i_size
      ,item_formulation i_formulation
      ,item_color i_color
      ,item_units i_units
      ,item_container i_container
      ,item_manager_id i_manager
      ,i_product_name
from s_item,
     item
where item_item_id = i_item_id
  and i_rec_end_date is null REFRESH MATERIALIZED VIEW itemv;"""

    nv_itemv = """ create view nitemv as
select  i_item_sk
      ,item_item_id i_item_id
      ,current_date i_rec_start_date
      ,cast(NULL as date) i_rec_end_date
      ,item_item_description i_item_desc
      ,item_list_price i_current_price
      ,item_wholesale_cost i_wholesalecost
      ,i_brand_id
      ,i_brand
      ,i_class_id
      ,i_class
      ,i_category_id
      ,i_category
      ,i_manufact_id
      ,i_manufact
      ,item_size i_size
      ,item_formulation i_formulation
      ,item_color i_color
      ,item_units i_units
      ,item_container i_container
      ,item_manager_id i_manager
      ,i_product_name
from s_item,
     item
where item_item_id = i_item_id
  and i_rec_end_date is null;"""

    mv_promv = """ create MATERIALIZED view promv as
select  prom_promotion_id p_promo_id
       ,d1.d_date_sk p_start_date_sk
       ,d2.d_date_sk p_end_date_sk
       ,prom_cost p_cost
       ,prom_response_target p_response_target
       ,prom_promotion_name p_promo_name
       ,prom_channel_dmail p_channel_dmail
       ,prom_channel_email p_channel_email
       ,prom_channel_catalog p_channel_catalog
       ,prom_channel_tv p_channel_tv
       ,prom_channel_radio p_channel_radio
       ,prom_channel_press p_channel_press
       ,prom_channel_event p_channel_event
       ,prom_channel_demo p_channel_demo
       ,prom_channel_details p_channel_details
       ,prom_purpose p_purpose
       ,prom_discount_active p_discount_active
from    s_promotion left outer join date_dim d1 on cast(prom_start_date as date) = d1.d_date
                    left outer join date_dim d2 on cast(prom_end_date as date) = d2.d_date REFRESH MATERIALIZED VIEW promv"""

    nv_promv = """create view npromv as
select  prom_promotion_id p_promo_id
       ,d1.d_date_sk p_start_date_sk
       ,d2.d_date_sk p_end_date_sk
       ,prom_cost p_cost
       ,prom_response_target p_response_target
       ,prom_promotion_name p_promo_name
       ,prom_channel_dmail p_channel_dmail
       ,prom_channel_email p_channel_email
       ,prom_channel_catalog p_channel_catalog
       ,prom_channel_tv p_channel_tv
       ,prom_channel_radio p_channel_radio
       ,prom_channel_press p_channel_press
       ,prom_channel_event p_channel_event
       ,prom_channel_demo p_channel_demo
       ,prom_channel_details p_channel_details
       ,prom_purpose p_purpose
       ,prom_discount_active p_discount_active
from    s_promotion left outer join date_dim d1 on cast(prom_start_date as date) = d1.d_date
                    left outer join date_dim d2 on cast(prom_end_date as date) = d2.d_date;"""


    mv_ccv = """create materialized  view  ccv as
select   cc_call_center_sk
        ,call_center_id cc_call_center_id
        ,current_date cc_rec_start_date
        ,cast(NULL as date) cc_rec_end_date
        ,d1.d_date_sk cc_closed_date_sk
        ,d2.d_date_sk cc_open_date_sk
        ,call_center_name cc_name
        ,call_center_class cc_class
        ,call_center_employees cc_employees
        ,call_center_sq_ft cc_sq_ft
        ,call_center_hours cc_hours
        ,call_center_manager cc_manager
        ,cc_mkt_id
        ,cc_mkt_class
        ,cc_mkt_desc
        ,cc_market_manager
        ,cc_division
        ,cc_division_name
        ,cc_company
        ,cc_company_name
        ,cc_street_number
        ,cc_street_name
        ,cc_street_type
        ,cc_suite_number
        ,cc_city
        ,cc_county
        ,cc_state
        ,cc_zip
        ,cc_country
        ,cc_gmt_offset
        ,call_center_tax_percentage cc_tax_percentage
from    s_call_center left outer join date_dim d2 on d2.d_date = cast(call_closed_date as date)
                      left outer join date_dim d1 on d1.d_date = cast(call_open_date as date),
        call_center
where  call_center_id = cc_call_center_id
   and cc_rec_end_date is null REFRESH MATERIALIZED VIEW ccv; """


    nv_ccv = """ create view  nccv as
select   cc_call_center_sk
        ,call_center_id cc_call_center_id
        ,current_date cc_rec_start_date
        ,cast(NULL as date) cc_rec_end_date
        ,d1.d_date_sk cc_closed_date_sk
        ,d2.d_date_sk cc_open_date_sk
        ,call_center_name cc_name
        ,call_center_class cc_class
        ,call_center_employees cc_employees
        ,call_center_sq_ft cc_sq_ft
        ,call_center_hours cc_hours
        ,call_center_manager cc_manager
        ,cc_mkt_id
        ,cc_mkt_class
        ,cc_mkt_desc
        ,cc_market_manager
        ,cc_division
        ,cc_division_name
        ,cc_company
        ,cc_company_name
        ,cc_street_number
        ,cc_street_name
        ,cc_street_type
        ,cc_suite_number
        ,cc_city
        ,cc_county
        ,cc_state
        ,cc_zip
        ,cc_country
        ,cc_gmt_offset
        ,call_center_tax_percentage cc_tax_percentage
from    s_call_center left outer join date_dim d2 on d2.d_date = cast(call_closed_date as date)
                      left outer join date_dim d1 on d1.d_date = cast(call_open_date as date),
        call_center
where  call_center_id = cc_call_center_id
   and cc_rec_end_date is null;"""

    mv_srv= """ create materialized view srv as
select d_date_sk sr_returned_date_sk
      ,t_time_sk sr_return_time_sk
      ,i_item_sk sr_item_sk
      ,c_customer_sk sr_customer_sk
      ,c_current_cdemo_sk sr_cdemo_sk
      ,c_current_hdemo_sk sr_hdemo_sk
      ,c_current_addr_sk sr_addr_sk
      ,s_store_sk sr_store_sk
      ,r_reason_sk sr_reason_sk
      ,sret_ticket_number sr_ticket_number
      ,sret_return_qty sr_return_quantity
      ,sret_return_amt sr_return_amt
      ,sret_return_tax sr_return_tax
      ,sret_return_amt + sret_return_tax sr_return_amt_inc_tax
      ,sret_return_fee sr_fee
      ,sret_return_ship_cost sr_return_ship_cost
      ,sret_refunded_cash sr_refunded_cash
      ,sret_reversed_charge sr_reversed_charde
      ,sret_store_credit sr_store_credit
      ,sret_return_amt+sret_return_tax+sret_return_fee
       -sret_refunded_cash-sret_reversed_charge-sret_store_credit sr_net_loss
from s_store_returns left outer join date_dim on (cast(sret_return_date as date) = d_date)
                       left outer join time_dim on (( cast(substring(sret_return_time,1,2) as integer)*3600
                                                     +cast(substring(sret_return_time,4,2) as integer)*60
                                                     +cast(substring(sret_return_time,7,2) as integer)) = t_time)
                     left outer join item on (sret_item_id = i_item_id)
                     left outer join customer on (sret_customer_id = c_customer_id)
                     left outer join store on (sret_store_id = s_store_id)
                     left outer join reason on (sret_reason_id = r_reason_id)
where i_rec_end_date is NULL
  and s_rec_end_date is NULL; REFRESH MATERIALIZED VIEW srv"""


    nv_srv = """ create view nsrv as
select d_date_sk sr_returned_date_sk
      ,t_time_sk sr_return_time_sk
      ,i_item_sk sr_item_sk
      ,c_customer_sk sr_customer_sk
      ,c_current_cdemo_sk sr_cdemo_sk
      ,c_current_hdemo_sk sr_hdemo_sk
      ,c_current_addr_sk sr_addr_sk
      ,s_store_sk sr_store_sk
      ,r_reason_sk sr_reason_sk
      ,sret_ticket_number sr_ticket_number
      ,sret_return_qty sr_return_quantity
      ,sret_return_amt sr_return_amt
      ,sret_return_tax sr_return_tax
      ,sret_return_amt + sret_return_tax sr_return_amt_inc_tax
      ,sret_return_fee sr_fee
      ,sret_return_ship_cost sr_return_ship_cost
      ,sret_refunded_cash sr_refunded_cash
      ,sret_reversed_charge sr_reversed_charde
      ,sret_store_credit sr_store_credit
      ,sret_return_amt+sret_return_tax+sret_return_fee
       -sret_refunded_cash-sret_reversed_charge-sret_store_credit sr_net_loss
from s_store_returns left outer join date_dim on (cast(sret_return_date as date) = d_date)
                       left outer join time_dim on (( cast(substring(sret_return_time,1,2) as integer)*3600
                                                     +cast(substring(sret_return_time,4,2) as integer)*60
                                                     +cast(substring(sret_return_time,7,2) as integer)) = t_time)
                     left outer join item on (sret_item_id = i_item_id)
                     left outer join customer on (sret_customer_id = c_customer_id)
                     left outer join store on (sret_store_id = s_store_id)
                     left outer join reason on (sret_reason_id = r_reason_id)
where i_rec_end_date is NULL
  and s_rec_end_date is NULL;"""

    mv_ssv = """ create materialized view ssv as
select  d_date_sk ss_sold_date_sk, 
        t_time_sk ss_sold_time_sk, 
        i_item_sk ss_item_sk, 
        c_customer_sk ss_customer_sk, 
        c_current_cdemo_sk ss_cdemo_sk, 
        c_current_hdemo_sk ss_hdemo_sk,
        c_current_addr_sk ss_addr_sk,
        s_store_sk ss_store_sk, 
        p_promo_sk ss_promo_sk,
        purc_purchase_id ss_ticket_number, 
        plin_quantity ss_quantity, 
        i_wholesale_cost ss_wholesale_cost, 
        i_current_price ss_list_price,
        plin_sale_price ss_sales_price,
        (i_current_price-plin_sale_price)*plin_quantity ss_ext_discount_amt,
        plin_sale_price * plin_quantity ss_ext_sales_price,
        i_wholesale_cost * plin_quantity ss_ext_wholesale_cost, 
        i_current_price * plin_quantity ss_ext_list_price, 
        i_current_price * s_tax_precentage ss_ext_tax, 
        plin_coupon_amt ss_coupon_amt,
        (plin_sale_price * plin_quantity)-plin_coupon_amt ss_net_paid,
        ((plin_sale_price * plin_quantity)-plin_coupon_amt)*(1+s_tax_precentage) ss_net_paid_inc_tax,
        ((plin_sale_price * plin_quantity)-plin_coupon_amt)-(plin_quantity*i_wholesale_cost) ss_net_profit
from    s_purchase left outer join customer on (purc_customer_id = c_customer_id) 
                     left outer join store on (purc_store_id = s_store_id)
                     left outer join date_dim on (cast(purc_purchase_date as date) = d_date)
                     left outer join time_dim on (PURC_PURCHASE_TIME = t_time),
        s_purchase_lineitem left outer join promotion on plin_promotion_id = p_promo_id
                           left outer join item on plin_item_id = i_item_id
where   purc_purchase_id = plin_purchase_id
    and i_rec_end_date is NULL
    and s_rec_end_date is NULL; REFRESH MATERIALIZED VIEW ssv"""

    nv_ssv= """ create view nssv as
select  d_date_sk ss_sold_date_sk, 
        t_time_sk ss_sold_time_sk, 
        i_item_sk ss_item_sk, 
        c_customer_sk ss_customer_sk, 
        c_current_cdemo_sk ss_cdemo_sk, 
        c_current_hdemo_sk ss_hdemo_sk,
        c_current_addr_sk ss_addr_sk,
        s_store_sk ss_store_sk, 
        p_promo_sk ss_promo_sk,
        purc_purchase_id ss_ticket_number, 
        plin_quantity ss_quantity, 
        i_wholesale_cost ss_wholesale_cost, 
        i_current_price ss_list_price,
        plin_sale_price ss_sales_price,
        (i_current_price-plin_sale_price)*plin_quantity ss_ext_discount_amt,
        plin_sale_price * plin_quantity ss_ext_sales_price,
        i_wholesale_cost * plin_quantity ss_ext_wholesale_cost, 
        i_current_price * plin_quantity ss_ext_list_price, 
        i_current_price * s_tax_precentage ss_ext_tax, 
        plin_coupon_amt ss_coupon_amt,
        (plin_sale_price * plin_quantity)-plin_coupon_amt ss_net_paid,
        ((plin_sale_price * plin_quantity)-plin_coupon_amt)*(1+s_tax_precentage) ss_net_paid_inc_tax,
        ((plin_sale_price * plin_quantity)-plin_coupon_amt)-(plin_quantity*i_wholesale_cost) ss_net_profit
from    s_purchase left outer join customer on (purc_customer_id = c_customer_id) 
                     left outer join store on (purc_store_id = s_store_id)
                     left outer join date_dim on (cast(purc_purchase_date as date) = d_date)
                     left outer join time_dim on (PURC_PURCHASE_TIME = t_time),
        s_purchase_lineitem left outer join promotion on plin_promotion_id = p_promo_id
                           left outer join item on plin_item_id = i_item_id
where   purc_purchase_id = plin_purchase_id
    and i_rec_end_date is NULL
    and s_rec_end_date is NULL;"""

    mv_storv = """create MATERIALIZED view storv as
select s_store_sk
      ,stor_store_id s_store_id
      ,current_date s_rec_start_date
      ,cast(NULL as date) s_rec_end_date
      ,d1.d_date_sk s_closed_date_sk
      ,stor_name s_store_name
      ,stor_employees s_number_employees
      ,stor_floor_space s_floor_space
      ,stor_hours s_hours
      ,stor_store_manager s_manager
      ,stor_market_id s_market_id
      ,stor_geography_class s_geography_class
      ,s_market_desc
      ,stor_market_manager s_market_manager
      ,s_division_id
      ,s_division_name
      ,s_company_id
      ,s_company_name
      ,s_street_number
      ,s_street_name
      ,s_street_type
      ,s_suite_number
      ,s_city
      ,s_county
      ,s_state
      ,s_zip
      ,s_country
      ,s_gmt_offset
      ,stor_tax_percentage s_tax_percentage
from  s_store left outer join date_dim d1 on cast(stor_closed_date as date)= d1.d_date
     ,store
where  stor_store_id = s_store_id
   and s_rec_end_date is null REFRESH MATERIALIZED VIEW storv; """

    nv_storv= """create view nstorv as
select s_store_sk
      ,stor_store_id s_store_id
      ,current_date s_rec_start_date
      ,cast(NULL as date) s_rec_end_date
      ,d1.d_date_sk s_closed_date_sk
      ,stor_name s_store_name
      ,stor_employees s_number_employees
      ,stor_floor_space s_floor_space
      ,stor_hours s_hours
      ,stor_store_manager s_manager
      ,stor_market_id s_market_id
      ,stor_geography_class s_geography_class
      ,s_market_desc
      ,stor_market_manager s_market_manager
      ,s_division_id
      ,s_division_name
      ,s_company_id
      ,s_company_name
      ,s_street_number
      ,s_street_name
      ,s_street_type
      ,s_suite_number
      ,s_city
      ,s_county
      ,s_state
      ,s_zip
      ,s_country
      ,s_gmt_offset
      ,stor_tax_percentage s_tax_percentage
from  s_store left outer join date_dim d1 on cast(stor_closed_date as date)= d1.d_date
     ,store
where  stor_store_id = s_store_id
   and s_rec_end_date is null;  """

    mv_websv = """create materialized view websv as
select  web_site_sk
      ,wsit_web_site_id web_site_id
      ,current_date web_rec_start_date
      ,cast(null as date) web_rec_end_date
      ,wsit_site_name web_name
      ,d1.d_date_sk web_open_date_sk
      ,d2.d_date_sk web_close_date_sk
      ,wsit_site_class web_class
      ,wsit_site_manager web_manager
      ,web_mkt_id 
      ,web_mkt_class
      ,web_mkt_desc
      ,web_market_manager
      ,web_company_id
      ,web_company_name
      ,web_street_number 
      ,web_street_name
      ,web_street_type 
      ,web_suite_number
      ,web_city
      ,web_county 
      ,web_state
      ,web_zip
      ,web_country 
      ,web_gmt_offset
      ,wsit_tax_percentage web_tax_percentage
from  s_web_site left outer join date_dim d1 on (d1.d_date = wsit_open_date)
                   left outer join date_dim d2 on (d2.d_date = wsit_closed_date), 
      web_site
where web_site_id = wsit_web_site_id
  and web_rec_end_date is null REFRESH MATERIALIZED VIEW websv; """


    nv_websv = """create view nwebsv as
select  web_site_sk
      ,wsit_web_site_id web_site_id
      ,current_date web_rec_start_date
      ,cast(null as date) web_rec_end_date
      ,wsit_site_name web_name
      ,d1.d_date_sk web_open_date_sk
      ,d2.d_date_sk web_close_date_sk
      ,wsit_site_class web_class
      ,wsit_site_manager web_manager
      ,web_mkt_id 
      ,web_mkt_class
      ,web_mkt_desc
      ,web_market_manager
      ,web_company_id
      ,web_company_name
      ,web_street_number 
      ,web_street_name
      ,web_street_type 
      ,web_suite_number
      ,web_city
      ,web_county 
      ,web_state
      ,web_zip
      ,web_country 
      ,web_gmt_offset
      ,wsit_tax_percentage web_tax_percentage
from  s_web_site left outer join date_dim d1 on (d1.d_date = wsit_open_date)
                   left outer join date_dim d2 on (d2.d_date = wsit_closed_date), 
      web_site
where web_site_id = wsit_web_site_id
  and web_rec_end_date is null; """


    mv_webv = """create materialized view webv as
select wp_web_page_sk,
      wpag_web_page_id wp_web_page_id
      ,current_date wp_rec_start_date
      ,cast(null as date) wp_rec_end_date
      ,d1.d_date_sk wp_creation_date_sk
      ,d2.d_date_sk wp_access_date_sk
      ,wpag_autogen_flag wp_autogen_flag
      ,wpag_url wp_url
      ,wpag_type wp_type 
      ,wpag_char_cnt wp_char_count
      ,wpag_link_cnt wp_link_count
      ,wpag_image_cnt wp_image_count
      ,wpag_max_ad_cnt wp_max_ad_count
from   web_page, s_web_page left outer join date_dim d1 on wpag_create_date  = d1.d_date
                     left outer join date_dim d2 on wpag_access_date  = d2.d_date REFRESH MATERIALIZED VIEW webv; """

    nv_webv = """create view nwebv as
select wp_web_page_sk,
      wpag_web_page_id wp_web_page_id
      ,current_date wp_rec_start_date
      ,cast(null as date) wp_rec_end_date
      ,d1.d_date_sk wp_creation_date_sk
      ,d2.d_date_sk wp_access_date_sk
      ,wpag_autogen_flag wp_autogen_flag
      ,wpag_url wp_url
      ,wpag_type wp_type 
      ,wpag_char_cnt wp_char_count
      ,wpag_link_cnt wp_link_count
      ,wpag_image_cnt wp_image_count
      ,wpag_max_ad_cnt wp_max_ad_count
from   web_page, s_web_page left outer join date_dim d1 on wpag_create_date  = d1.d_date
                     left outer join date_dim d2 on wpag_access_date  = d2.d_date;"""

    mv_wrhsv = """create MATERIALIZED view wrhsv as
select  wrhs_warehouse_id w_warehouse_id
       ,wrhs_warehouse_desc w_warehouse_name
       ,wrhs_warehouse_sq_ft w_warehouse_sq_ft
       ,w_street_number
       ,w_street_name
       ,w_street_type
       ,w_suite_number
       ,w_city
       ,w_county
       ,w_state
       ,w_zip
       ,w_country
       ,w_gmt_offset
from    s_warehouse,
        warehouse
where   wrhs_warehouse_id = w_warehouse_id REFRESH MATERIALIZED VIEW wrhsv;"""

    nv_wrhsv = """create view nwrhsv as
select  wrhs_warehouse_id w_warehouse_id
       ,wrhs_warehouse_desc w_warehouse_name
       ,wrhs_warehouse_sq_ft w_warehouse_sq_ft
       ,w_street_number
       ,w_street_name
       ,w_street_type
       ,w_suite_number
       ,w_city
       ,w_county
       ,w_state
       ,w_zip
       ,w_country
       ,w_gmt_offset
from    s_warehouse,
        warehouse
where   wrhs_warehouse_id = w_warehouse_id;"""

    mv_wrv = """create materialized view wrv as
select d_date_sk wr_return_date_sk
      ,t_time_sk wr_return_time_sk
      ,i_item_sk wr_item_sk
      ,c1.c_customer_sk wr_refunded_customer_sk
      ,c1.c_current_cdemo_sk wr_refunded_cdemo_sk
      ,c1.c_current_hdemo_sk wr_refunded_hdemo_sk
      ,c1.c_current_addr_sk wr_refunded_addr_sk
      ,c2.c_customer_sk wr_returning_customer_sk
      ,c2.c_current_cdemo_sk wr_returning_cdemo_sk
      ,c2.c_current_hdemo_sk wr_returning_hdemo_sk
      ,c2.c_current_addr_sk wr_returing_addr_sk
      ,wp_web_page_sk wr_web_page_sk 
      ,r_reason_sk wr_reason_sk
      ,wret_order_id wr_order_number
      ,wret_return_qty wr_return_quantity
      ,wret_return_amt wr_return_amt
      ,wret_return_tax wr_return_tax
      ,wret_return_amt + wret_return_tax as wr_return_amt_inc_tax
      ,wret_return_fee wr_fee
      ,wret_return_ship_cost wr_return_ship_cost
      ,wret_refunded_cash wr_refunded_cash
      ,wret_reversed_charge wr_reversed_charde
      ,wret_account_credit wr_account_credit
      ,wret_return_amt+wret_return_tax+wret_return_fee
       -wret_refunded_cash-wret_reversed_charge-wret_account_credit wr_net_loss
from s_web_returns left outer join date_dim on (cast(wret_return_date as date) = d_date)
                   left outer join time_dim on (( cast(substring(wret_return_time,1,2) as integer)*3600
                                                 +cast(substring(wret_return_time,4,2) as integer)*60
                                                 +cast(substring(wret_return_time,7,2) as integer)) = t_time)
                   left outer join item on (wret_item_id = i_item_id)
                   left outer join customer c1 on (wret_return_customer_id = c1.c_customer_id)
                   left outer join customer c2 on (wret_refund_customer_id = c2.c_customer_id)
                   left outer join reason on (wret_reason_id = r_reason_id)
                   left outer join web_page on (wret_web_site_id = WP_WEB_PAGE_id)
where i_rec_end_date is NULL
  and wp_rec_end_date is NULL REFRESH MATERIALIZED VIEW wrv;"""

    nv_wrv = """create view nwrv as
select d_date_sk wr_return_date_sk
      ,t_time_sk wr_return_time_sk
      ,i_item_sk wr_item_sk
      ,c1.c_customer_sk wr_refunded_customer_sk
      ,c1.c_current_cdemo_sk wr_refunded_cdemo_sk
      ,c1.c_current_hdemo_sk wr_refunded_hdemo_sk
      ,c1.c_current_addr_sk wr_refunded_addr_sk
      ,c2.c_customer_sk wr_returning_customer_sk
      ,c2.c_current_cdemo_sk wr_returning_cdemo_sk
      ,c2.c_current_hdemo_sk wr_returning_hdemo_sk
      ,c2.c_current_addr_sk wr_returing_addr_sk
      ,wp_web_page_sk wr_web_page_sk 
      ,r_reason_sk wr_reason_sk
      ,wret_order_id wr_order_number
      ,wret_return_qty wr_return_quantity
      ,wret_return_amt wr_return_amt
      ,wret_return_tax wr_return_tax
      ,wret_return_amt + wret_return_tax as wr_return_amt_inc_tax
      ,wret_return_fee wr_fee
      ,wret_return_ship_cost wr_return_ship_cost
      ,wret_refunded_cash wr_refunded_cash
      ,wret_reversed_charge wr_reversed_charde
      ,wret_account_credit wr_account_credit
      ,wret_return_amt+wret_return_tax+wret_return_fee
       -wret_refunded_cash-wret_reversed_charge-wret_account_credit wr_net_loss
from s_web_returns left outer join date_dim on (cast(wret_return_date as date) = d_date)
                   left outer join time_dim on (( cast(substring(wret_return_time,1,2) as integer)*3600
                                                 +cast(substring(wret_return_time,4,2) as integer)*60
                                                 +cast(substring(wret_return_time,7,2) as integer)) = t_time)
                   left outer join item on (wret_item_id = i_item_id)
                   left outer join customer c1 on (wret_return_customer_id = c1.c_customer_id)
                   left outer join customer c2 on (wret_refund_customer_id = c2.c_customer_id)
                   left outer join reason on (wret_reason_id = r_reason_id)
                   left outer join web_page on (wret_web_site_id = WP_WEB_PAGE_id)
where i_rec_end_date is NULL
  and wp_rec_end_date is NULL;"""

    mv_wsv = """create materialized view wsv as
select  d1.d_date_sk ws_sold_date_sk, 
        t_time_sk ws_sold_time_sk, 
        d2.d_date_sk ws_ship_date_sk,
        i_item_sk ws_item_sk, 
        c1.c_customer_sk ws_bill_customer_sk, 
        c1.c_current_cdemo_sk ws_bill_cdemo_sk, 
        c1.c_current_hdemo_sk ws_bill_hdemo_sk,
        c1.c_current_addr_sk ws_bill_addr_sk,
        c2.c_customer_sk ws_ship_customer_sk,
        c2.c_current_cdemo_sk ws_ship_cdemo_sk,
        c2.c_current_hdemo_sk ws_ship_hdemo_sk,
        c2.c_current_addr_sk ws_ship_addr_sk,
        web_site_sk ws_web_page_sk,
        wp_web_page_sk ws_web_site_s,
        sm_ship_mode_sk ws_ship_mode_sk,
        w_warehouse_sk ws_warehouse_sk,
        p_promo_sk ws_promo_sk,
        word_order_id ws_order_number, 
        wlin_quantity ws_quantity, 
        i_wholesale_cost ws_wholesale_cost, 
        i_current_price ws_list_price,
        wlin_sales_price ws_sales_price,
        (i_current_price-wlin_sales_price)*wlin_quantity ws_ext_discount_amt,
        wlin_sales_price * wlin_quantity ws_ext_sales_price,
        i_wholesale_cost * wlin_quantity ws_ext_wholesale_cost, 
        i_current_price * wlin_quantity ws_ext_list_price, 
        i_current_price * web_tax_percentage ws_ext_tax,  
        wlin_coupon_amt ws_coupon_amt,
        0 WS_EXT_SHIP_COST,
        (wlin_sales_price * wlin_quantity)-wlin_coupon_amt ws_net_paid,
        ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)*(1+web_tax_percentage) ws_net_paid_inc_tax,
        ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)-(wlin_quantity*i_wholesale_cost) WS_NET_PAID_INC_SHIP,
        (wlin_sales_price * wlin_quantity)-wlin_coupon_amt + (wlin_ship_cost * wlin_quantity)
        + i_current_price * web_tax_percentage WS_NET_PAID_INC_SHIP_TAX,
        ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)-(i_wholesale_cost * wlin_quantity) WS_NET_PROFIT
from    s_web_order left outer join date_dim d1 on (cast(word_order_date as date) =  d1.d_date)
                    left outer join time_dim on (word_order_time = t_time)
                    left outer join customer c1 on (word_bill_customer_id = c1.c_customer_id)
                    left outer join customer c2 on (word_ship_customer_id = c2.c_customer_id)
                    left outer join web_site on (word_web_site_id = web_site_id)
                    left outer join ship_mode on (word_ship_mode_id = sm_ship_mode_id), 
        s_web_order_lineitem left outer join date_dim d2 on (cast(wlin_ship_date as date) = d2.d_date)
                             left outer join item on (wlin_item_id = i_item_id)
                             left outer join web_page on (wlin_web_page_id = wp_web_page_id)
                             left outer join warehouse on (wlin_warehouse_id = w_warehouse_id)
                             left outer join promotion on (wlin_promotion_id = p_promo_id)
where   word_order_id = wlin_order_id
    and i_rec_end_date is NULL 
    and web_rec_end_date is null
    and wp_rec_end_date is null REFRESH MATERIALIZED VIEW wsv; """


    nv_wsv = """create view Nwsv as
select  d1.d_date_sk ws_sold_date_sk, 
        t_time_sk ws_sold_time_sk, 
        d2.d_date_sk ws_ship_date_sk,
        i_item_sk ws_item_sk, 
        c1.c_customer_sk ws_bill_customer_sk, 
        c1.c_current_cdemo_sk ws_bill_cdemo_sk, 
        c1.c_current_hdemo_sk ws_bill_hdemo_sk,
        c1.c_current_addr_sk ws_bill_addr_sk,
        c2.c_customer_sk ws_ship_customer_sk,
        c2.c_current_cdemo_sk ws_ship_cdemo_sk,
        c2.c_current_hdemo_sk ws_ship_hdemo_sk,
        c2.c_current_addr_sk ws_ship_addr_sk,
        web_site_sk ws_web_page_sk,
        wp_web_page_sk ws_web_site_s,
        sm_ship_mode_sk ws_ship_mode_sk,
        w_warehouse_sk ws_warehouse_sk,
        p_promo_sk ws_promo_sk,
        word_order_id ws_order_number, 
        wlin_quantity ws_quantity, 
        i_wholesale_cost ws_wholesale_cost, 
        i_current_price ws_list_price,
        wlin_sales_price ws_sales_price,
        (i_current_price-wlin_sales_price)*wlin_quantity ws_ext_discount_amt,
        wlin_sales_price * wlin_quantity ws_ext_sales_price,
        i_wholesale_cost * wlin_quantity ws_ext_wholesale_cost, 
        i_current_price * wlin_quantity ws_ext_list_price, 
        i_current_price * web_tax_percentage ws_ext_tax,  
        wlin_coupon_amt ws_coupon_amt,
        0 WS_EXT_SHIP_COST,
        (wlin_sales_price * wlin_quantity)-wlin_coupon_amt ws_net_paid,
        ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)*(1+web_tax_percentage) ws_net_paid_inc_tax,
        ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)-(wlin_quantity*i_wholesale_cost) WS_NET_PAID_INC_SHIP,
        (wlin_sales_price * wlin_quantity)-wlin_coupon_amt + (wlin_ship_cost * wlin_quantity)
        + i_current_price * web_tax_percentage WS_NET_PAID_INC_SHIP_TAX,
        ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)-(i_wholesale_cost * wlin_quantity) WS_NET_PROFIT
from    s_web_order left outer join date_dim d1 on (cast(word_order_date as date) =  d1.d_date)
                    left outer join time_dim on (word_order_time = t_time)
                    left outer join customer c1 on (word_bill_customer_id = c1.c_customer_id)
                    left outer join customer c2 on (word_ship_customer_id = c2.c_customer_id)
                    left outer join web_site on (word_web_site_id = web_site_id)
                    left outer join ship_mode on (word_ship_mode_id = sm_ship_mode_id), 
        s_web_order_lineitem left outer join date_dim d2 on (cast(wlin_ship_date as date) = d2.d_date)
                             left outer join item on (wlin_item_id = i_item_id)
                             left outer join web_page on (wlin_web_page_id = wp_web_page_id)
                             left outer join warehouse on (wlin_warehouse_id = w_warehouse_id)
                             left outer join promotion on (wlin_promotion_id = p_promo_id)
where   word_order_id = wlin_order_id
    and i_rec_end_date is NULL 
    and web_rec_end_date is null
    and wp_rec_end_date is null; """

    cursor.execute(mv_crv)
    cursor.execute(nv_crv)
    cursor.execute(mv_csv)
    cursor.execute(nv_csv)
    cursor.execute(mv_itemv)
    cursor.execute(nv_itemv)
    cursor.execute(mv_promv)
    cursor.execute(nv_promv)
    cursor.execute(mv_ccv)
    cursor.execute(nv_ccv)
    cursor.execute(mv_srv)
    cursor.execute(nv_srv)
    cursor.execute(mv_ssv)
    cursor.execute(nv_ssv)
    cursor.execute(mv_storv)
    cursor.execute(nv_storv)
    cursor.execute(mv_websv)
    cursor.execute(nv_websv)
    cursor.execute(mv_webv)
    cursor.execute(nv_webv)
    cursor.execute(mv_wrhsv)
    cursor.execute(nv_wrhsv)
    cursor.execute(mv_wrv)
    cursor.execute(nv_wrv)
    cursor.execute(mv_wsv)
    cursor.execute(nv_wsv)

    #Storing sql queries
    mv_sql0 = """insert into catalog_returns select * from crv;"""
    nv_sql0 = """insert into catalog_returns select * from ncrv;"""
    mv_sql1 = """insert into catalog_sales select * from csv"""
    nv_sql1 = """insert into catalog_sales select * from ncsv """
    mv_sql2 = """insert into item select * from itemv """
    nv_sql2 = """insert into item select * from nitemv """
    mv_sql3 = """select count(*) from promv s,promotion d where s.P_PROMO_ID=d.P_PROMO_ID; """
    nv_sql3 = """select count(*) from npromv s,promotion d where s.P_PROMO_ID=d.P_PROMO_ID; """
    mv_sql4 = """insert into call_center select * from ccv; """
    nv_sql4 = """insert into call_center select * from nccv; """
    mv_sql5 = """insert into store_returns select * from srv """
    nv_sql5 = """insert into store_returns select * from nsrv """
    mv_sql6 = """insert into store_sales select * from ssv """
    nv_sql6 = """insert into store_sales select * from nssv """
    mv_sql7 = """insert into store select * from storv """
    nv_sql7 = """insert into store select * from nstorv """
    mv_sql8 = """insert into web_site select * from websv """
    nv_sql8 = """insert into web_site select * from nwebsv """
    mv_sql9 = """insert into web_page select * from webv """
    nv_sql9 = """ insert into web_page select * from nwebv"""
    mv_sql10 = """select count(*) from wrhsv; """
    nv_sql10 = """select count(*) from nwrhsv; """
    mv_sql11 = """insert into web_returns select * from wrv """
    nv_sql11 = """ insert into web_returns select * from nwrv"""
    mv_sql12 = """insert into web_sales select * from wsv """
    nv_sql12 = """insert into web_sales select * from wsv """


    a = datetime.datetime.now()
    cursor.execute(mv_sql0)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized crv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql0)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized crv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql1)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized csv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql1)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized csv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql2)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized itemv ",int(delta.total_seconds()*10000))
    
    a = datetime.datetime.now()
    cursor.execute(nv_sql2)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized itemv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql3)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized promv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql3)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized promv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql4)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized ccv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql4)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized ccv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql5)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized srv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql5)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized srv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql6)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized ssv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql6)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized ssv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql7)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized storv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql7)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized storv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql8)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized websv ",int(delta.total_seconds()*10000))


    a = datetime.datetime.now()
    cursor.execute(nv_sql8)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized websv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql9)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized webv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql9)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized webv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql10)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized wrhsv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql10)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized wrhsv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql11)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized wrv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql11)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized wrv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql12)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized wsv ",int(delta.total_seconds()*10000))


    a = datetime.datetime.now()
    cursor.execute(nv_sql12)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized wsv ",int(delta.total_seconds()*10000))

def OracleSQL():
    connection = cx_Oracle.connect('zeeshan/tpcds@localhost') 

    cursor = connection.cursor()




    mv_crv = """ create materialized view crv BUILD IMMEDIATE 
 as
select d_date_sk cr_return_date_sk
      ,t_time_sk cr_return_time_sk
      ,0 CR_SHIP_DATE_SK
      ,i_item_sk cr_item_sk
      ,c1.c_customer_sk cr_refunded_customer_sk
      ,c1.c_current_cdemo_sk cr_refunded_cdemo_sk
      ,c1.c_current_hdemo_sk cr_refunded_hdemo_sk
      ,c1.c_current_addr_sk cr_refunded_addr_sk
      ,c2.c_customer_sk cr_returning_customer_sk
      ,c2.c_current_cdemo_sk cr_returning_cdemo_sk
      ,c2.c_current_hdemo_sk cr_returning_hdemo_sk
      ,c2.c_current_addr_sk cr_returing_addr_sk
      ,cc_call_center_sk cr_call_center_sk
      ,0 CR_CATALOG_PAGE_SK
      ,0 CR_SHIP_MODE_SK
      ,0 CR_WAREHOUSE_SK
      ,r_reason_sk cr_reason_sk
      ,cret_order_id cr_order_number
      ,cret_return_qty cr_return_quantity
      ,cret_return_amt cr_return_amt
      ,cret_return_tax cr_return_tax
      ,cret_return_amt + cret_return_tax as cr_return_amt_inc_tax
      ,cret_return_fee cr_fee
      ,cret_return_ship_cost cr_return_ship_cost
      ,cret_refunded_cash cr_refunded_cash
      ,cret_reversed_charge cr_reversed_charde
      ,cret_merchant_credit cr_merchant_credit
      ,cret_return_amt+cret_return_tax+cret_return_fee
         -cret_refunded_cash-cret_reversed_charge-cret_merchant_credit cr_net_loss
from s_catalog_returns left outer join date_dim on (cast(cret_return_date as date) = d_date)
                       left outer join time_dim on (( cast(substring(cret_return_time,1,2) as integer)*3600
                                                     +cast(substring(cret_return_time,4,2) as integer)*60
                                                     +cast(substring(cret_return_time,7,2) as integer)) = t_time)
                       left outer join item on (cret_item_id = i_item_id)
                       left outer join customer c1 on (cret_return_customer_id = c1.c_customer_id)
                       left outer join customer c2 on (cret_refund_customer_id = c2.c_customer_id)
                       left outer join reason on (cret_reason_id = r_reason_id)
                       left outer join call_center on (cret_call_center_id = cc_call_center_id)
where i_rec_end_date is NULL
  and cc_rec_end_date is NULL;"""




    nv_crv = """ create view ncrv as
select d_date_sk cr_return_date_sk
      ,t_time_sk cr_return_time_sk
      ,0 CR_SHIP_DATE_SK
      ,i_item_sk cr_item_sk
      ,c1.c_customer_sk cr_refunded_customer_sk
      ,c1.c_current_cdemo_sk cr_refunded_cdemo_sk
      ,c1.c_current_hdemo_sk cr_refunded_hdemo_sk
      ,c1.c_current_addr_sk cr_refunded_addr_sk
      ,c2.c_customer_sk cr_returning_customer_sk
      ,c2.c_current_cdemo_sk cr_returning_cdemo_sk
      ,c2.c_current_hdemo_sk cr_returning_hdemo_sk
      ,c2.c_current_addr_sk cr_returing_addr_sk
      ,cc_call_center_sk cr_call_center_sk
      ,0 CR_CATALOG_PAGE_SK
      ,0 CR_SHIP_MODE_SK
      ,0 CR_WAREHOUSE_SK
      ,r_reason_sk cr_reason_sk
      ,cret_order_id cr_order_number
      ,cret_return_qty cr_return_quantity
      ,cret_return_amt cr_return_amt
      ,cret_return_tax cr_return_tax
      ,cret_return_amt + cret_return_tax as cr_return_amt_inc_tax
      ,cret_return_fee cr_fee
      ,cret_return_ship_cost cr_return_ship_cost
      ,cret_refunded_cash cr_refunded_cash
      ,cret_reversed_charge cr_reversed_charde
      ,cret_merchant_credit cr_merchant_credit
      ,cret_return_amt+cret_return_tax+cret_return_fee
         -cret_refunded_cash-cret_reversed_charge-cret_merchant_credit cr_net_loss
from s_catalog_returns left outer join date_dim on (cast(cret_return_date as date) = d_date)
                       left outer join time_dim on (( cast(substring(cret_return_time,1,2) as integer)*3600
                                                     +cast(substring(cret_return_time,4,2) as integer)*60
                                                     +cast(substring(cret_return_time,7,2) as integer)) = t_time)
                       left outer join item on (cret_item_id = i_item_id)
                       left outer join customer c1 on (cret_return_customer_id = c1.c_customer_id)
                       left outer join customer c2 on (cret_refund_customer_id = c2.c_customer_id)
                       left outer join reason on (cret_reason_id = r_reason_id)
                       left outer join call_center on (cret_call_center_id = cc_call_center_id)
where i_rec_end_date is NULL
  and cc_rec_end_date is NULL;"""

    mv_csv = """ create materialized view csv BUILD IMMEDIATE 
 as
select d1.d_date_sk cs_sold_date_sk 
      ,t_time_sk cs_sold_time_sk 
      ,d2.d_date_sk cs_ship_date_sk
      ,c1.c_customer_sk cs_bill_customer_sk
      ,c1.c_current_cdemo_sk cs_bill_cdemo_sk 
      ,c1.c_current_hdemo_sk cs_bill_hdemo_sk
      ,c1.c_current_addr_sk cs_bill_addr_sk
      ,c2.c_customer_sk cs_ship_customer_sk
      ,c2.c_current_cdemo_sk cs_ship_cdemo_sk
      ,c2.c_current_hdemo_sk cs_ship_hdemo_sk
      ,c2.c_current_addr_sk cs_ship_addr_sk
      ,cc_call_center_sk cs_call_center
      ,cp_catalog_page_sk cs_catalog_page_sk
      ,sm_ship_mode_sk cs_ship_mode_sk
      ,w_warehouse_sk cs_warehouse_sk
      ,i_item_sk cs_item_sk
      ,p_promo_sk cs_promo_sk
      ,cord_order_id cs_order_number
      ,clin_quantity cs_quantity
      ,i_wholesale_cost cs_wholesale_cost
      ,i_current_price cs_list_price
      ,clin_sales_price cs_sales_price
      ,(i_current_price-clin_sales_price)*clin_quantity cs_ext_discount_amt
      ,clin_sales_price * clin_quantity cs_ext_sales_price
      ,i_wholesale_cost * clin_quantity cs_ext_wholesale_cost 
      ,i_current_price * clin_quantity CS_EXT_LIST_PRICE
      ,i_current_price * cc_tax_percentage CS_EXT_TAX
      ,clin_coupon_amt cs_coupon_amt
      ,clin_ship_cost * clin_quantity CS_EXT_SHIP_COST
      ,(clin_sales_price * clin_quantity)-clin_coupon_amt cs_net_paid
      ,((clin_sales_price * clin_quantity)-clin_coupon_amt)*(1+cc_tax_percentage) cs_net_paid_inc_tax
      ,(clin_sales_price * clin_quantity)-clin_coupon_amt + (clin_ship_cost * clin_quantity) CS_NET_PAID_INC_SHIP
      ,(clin_sales_price * clin_quantity)-clin_coupon_amt + (clin_ship_cost * clin_quantity) 
       + i_current_price * cc_tax_percentage CS_NET_PAID_INC_SHIP_TAX
      ,((clin_sales_price * clin_quantity)-clin_coupon_amt)-(clin_quantity*i_wholesale_cost) cs_net_profit
from    s_catalog_order left outer join date_dim d1 on (cast(cord_order_date as date) = d1.d_date)
                          left outer join time_dim on (cord_order_time = t_time)
                          left outer join customer c1 on (cord_bill_customer_id = c1.c_customer_id)
                          left outer join customer c2 on (cord_ship_customer_id = c2.c_customer_id)
                          left outer join call_center on (cord_call_center_id = cc_call_center_id)
                          left outer join ship_mode on (cord_ship_mode_id = sm_ship_mode_id), 
        s_catalog_order_lineitem
                          left outer join date_dim d2 on (cast(clin_ship_date as date) = d2.d_date)
                          left outer join catalog_page on (clin_catalog_page_number = cp_catalog_page_number and
                                                           clin_catalog_number = cp_catalog_number)
                          left outer join warehouse on (clin_warehouse_id = w_warehouse_id)
                          left outer join item on (clin_item_id = i_item_id)
                          left outer join promotion on (clin_promotion_id = p_promo_id)
where   cord_order_id = clin_order_id
    and i_rec_end_date is NULL 
    and cc_rec_end_date is null;
	"""

    nv_csv = """ create view ncsv as
select d1.d_date_sk cs_sold_date_sk 
      ,t_time_sk cs_sold_time_sk 
      ,d2.d_date_sk cs_ship_date_sk
      ,c1.c_customer_sk cs_bill_customer_sk
      ,c1.c_current_cdemo_sk cs_bill_cdemo_sk 
      ,c1.c_current_hdemo_sk cs_bill_hdemo_sk
      ,c1.c_current_addr_sk cs_bill_addr_sk
      ,c2.c_customer_sk cs_ship_customer_sk
      ,c2.c_current_cdemo_sk cs_ship_cdemo_sk
      ,c2.c_current_hdemo_sk cs_ship_hdemo_sk
      ,c2.c_current_addr_sk cs_ship_addr_sk
      ,cc_call_center_sk cs_call_center
      ,cp_catalog_page_sk cs_catalog_page_sk
      ,sm_ship_mode_sk cs_ship_mode_sk
      ,w_warehouse_sk cs_warehouse_sk
      ,i_item_sk cs_item_sk
      ,p_promo_sk cs_promo_sk
      ,cord_order_id cs_order_number
      ,clin_quantity cs_quantity
      ,i_wholesale_cost cs_wholesale_cost
      ,i_current_price cs_list_price
      ,clin_sales_price cs_sales_price
      ,(i_current_price-clin_sales_price)*clin_quantity cs_ext_discount_amt
      ,clin_sales_price * clin_quantity cs_ext_sales_price
      ,i_wholesale_cost * clin_quantity cs_ext_wholesale_cost 
      ,i_current_price * clin_quantity CS_EXT_LIST_PRICE
      ,i_current_price * cc_tax_percentage CS_EXT_TAX
      ,clin_coupon_amt cs_coupon_amt
      ,clin_ship_cost * clin_quantity CS_EXT_SHIP_COST
      ,(clin_sales_price * clin_quantity)-clin_coupon_amt cs_net_paid
      ,((clin_sales_price * clin_quantity)-clin_coupon_amt)*(1+cc_tax_percentage) cs_net_paid_inc_tax
      ,(clin_sales_price * clin_quantity)-clin_coupon_amt + (clin_ship_cost * clin_quantity) CS_NET_PAID_INC_SHIP
      ,(clin_sales_price * clin_quantity)-clin_coupon_amt + (clin_ship_cost * clin_quantity) 
       + i_current_price * cc_tax_percentage CS_NET_PAID_INC_SHIP_TAX
      ,((clin_sales_price * clin_quantity)-clin_coupon_amt)-(clin_quantity*i_wholesale_cost) cs_net_profit
from    s_catalog_order left outer join date_dim d1 on (cast(cord_order_date as date) = d1.d_date)
                          left outer join time_dim on (cord_order_time = t_time)
                          left outer join customer c1 on (cord_bill_customer_id = c1.c_customer_id)
                          left outer join customer c2 on (cord_ship_customer_id = c2.c_customer_id)
                          left outer join call_center on (cord_call_center_id = cc_call_center_id)
                          left outer join ship_mode on (cord_ship_mode_id = sm_ship_mode_id), 
        s_catalog_order_lineitem
                          left outer join date_dim d2 on (cast(clin_ship_date as date) = d2.d_date)
                          left outer join catalog_page on (clin_catalog_page_number = cp_catalog_page_number and
                                                           clin_catalog_number = cp_catalog_number)
                          left outer join warehouse on (clin_warehouse_id = w_warehouse_id)
                          left outer join item on (clin_item_id = i_item_id)
                          left outer join promotion on (clin_promotion_id = p_promo_id)
where   cord_order_id = clin_order_id
    and i_rec_end_date is NULL 
    and cc_rec_end_date is null;
	"""

    mv_itemv = """ create materialized view itemv BUILD IMMEDIATE 
 as
select  i_item_sk
      ,item_item_id i_item_id
      ,current_date i_rec_start_date
      ,cast(NULL as date) i_rec_end_date
      ,item_item_description i_item_desc
      ,item_list_price i_current_price
      ,item_wholesale_cost i_wholesalecost
      ,i_brand_id
      ,i_brand
      ,i_class_id
      ,i_class
      ,i_category_id
      ,i_category
      ,i_manufact_id
      ,i_manufact
      ,item_size i_size
      ,item_formulation i_formulation
      ,item_color i_color
      ,item_units i_units
      ,item_container i_container
      ,item_manager_id i_manager
      ,i_product_name
from s_item,
     item
where item_item_id = i_item_id
  and i_rec_end_date is null;"""

    nv_itemv = """ create view nitemv as
select  i_item_sk
      ,item_item_id i_item_id
      ,current_date i_rec_start_date
      ,cast(NULL as date) i_rec_end_date
      ,item_item_description i_item_desc
      ,item_list_price i_current_price
      ,item_wholesale_cost i_wholesalecost
      ,i_brand_id
      ,i_brand
      ,i_class_id
      ,i_class
      ,i_category_id
      ,i_category
      ,i_manufact_id
      ,i_manufact
      ,item_size i_size
      ,item_formulation i_formulation
      ,item_color i_color
      ,item_units i_units
      ,item_container i_container
      ,item_manager_id i_manager
      ,i_product_name
from s_item,
     item
where item_item_id = i_item_id
  and i_rec_end_date is null;"""

    mv_promv = """ create MATERIALIZED view promv BUILD IMMEDIATE 
 as
select  prom_promotion_id p_promo_id
       ,d1.d_date_sk p_start_date_sk
       ,d2.d_date_sk p_end_date_sk
       ,prom_cost p_cost
       ,prom_response_target p_response_target
       ,prom_promotion_name p_promo_name
       ,prom_channel_dmail p_channel_dmail
       ,prom_channel_email p_channel_email
       ,prom_channel_catalog p_channel_catalog
       ,prom_channel_tv p_channel_tv
       ,prom_channel_radio p_channel_radio
       ,prom_channel_press p_channel_press
       ,prom_channel_event p_channel_event
       ,prom_channel_demo p_channel_demo
       ,prom_channel_details p_channel_details
       ,prom_purpose p_purpose
       ,prom_discount_active p_discount_active
from    s_promotion left outer join date_dim d1 on cast(prom_start_date as date) = d1.d_date
                    left outer join date_dim d2 on cast(prom_end_date as date) = d2.d_date;"""

    nv_promv = """create view npromv as
select  prom_promotion_id p_promo_id
       ,d1.d_date_sk p_start_date_sk
       ,d2.d_date_sk p_end_date_sk
       ,prom_cost p_cost
       ,prom_response_target p_response_target
       ,prom_promotion_name p_promo_name
       ,prom_channel_dmail p_channel_dmail
       ,prom_channel_email p_channel_email
       ,prom_channel_catalog p_channel_catalog
       ,prom_channel_tv p_channel_tv
       ,prom_channel_radio p_channel_radio
       ,prom_channel_press p_channel_press
       ,prom_channel_event p_channel_event
       ,prom_channel_demo p_channel_demo
       ,prom_channel_details p_channel_details
       ,prom_purpose p_purpose
       ,prom_discount_active p_discount_active
from    s_promotion left outer join date_dim d1 on cast(prom_start_date as date) = d1.d_date
                    left outer join date_dim d2 on cast(prom_end_date as date) = d2.d_date;"""


    mv_ccv = """create materialized  view  ccv BUILD IMMEDIATE 
 as
select   cc_call_center_sk
        ,call_center_id cc_call_center_id
        ,current_date cc_rec_start_date
        ,cast(NULL as date) cc_rec_end_date
        ,d1.d_date_sk cc_closed_date_sk
        ,d2.d_date_sk cc_open_date_sk
        ,call_center_name cc_name
        ,call_center_class cc_class
        ,call_center_employees cc_employees
        ,call_center_sq_ft cc_sq_ft
        ,call_center_hours cc_hours
        ,call_center_manager cc_manager
        ,cc_mkt_id
        ,cc_mkt_class
        ,cc_mkt_desc
        ,cc_market_manager
        ,cc_division
        ,cc_division_name
        ,cc_company
        ,cc_company_name
        ,cc_street_number
        ,cc_street_name
        ,cc_street_type
        ,cc_suite_number
        ,cc_city
        ,cc_county
        ,cc_state
        ,cc_zip
        ,cc_country
        ,cc_gmt_offset
        ,call_center_tax_percentage cc_tax_percentage
from    s_call_center left outer join date_dim d2 on d2.d_date = cast(call_closed_date as date)
                      left outer join date_dim d1 on d1.d_date = cast(call_open_date as date),
        call_center
where  call_center_id = cc_call_center_id
   and cc_rec_end_date is null; """


    nv_ccv = """ create view  nccv as
select   cc_call_center_sk
        ,call_center_id cc_call_center_id
        ,current_date cc_rec_start_date
        ,cast(NULL as date) cc_rec_end_date
        ,d1.d_date_sk cc_closed_date_sk
        ,d2.d_date_sk cc_open_date_sk
        ,call_center_name cc_name
        ,call_center_class cc_class
        ,call_center_employees cc_employees
        ,call_center_sq_ft cc_sq_ft
        ,call_center_hours cc_hours
        ,call_center_manager cc_manager
        ,cc_mkt_id
        ,cc_mkt_class
        ,cc_mkt_desc
        ,cc_market_manager
        ,cc_division
        ,cc_division_name
        ,cc_company
        ,cc_company_name
        ,cc_street_number
        ,cc_street_name
        ,cc_street_type
        ,cc_suite_number
        ,cc_city
        ,cc_county
        ,cc_state
        ,cc_zip
        ,cc_country
        ,cc_gmt_offset
        ,call_center_tax_percentage cc_tax_percentage
from    s_call_center left outer join date_dim d2 on d2.d_date = cast(call_closed_date as date)
                      left outer join date_dim d1 on d1.d_date = cast(call_open_date as date),
        call_center
where  call_center_id = cc_call_center_id
   and cc_rec_end_date is null;"""

    mv_srv= """ create materialized view srv BUILD IMMEDIATE 
 as
select d_date_sk sr_returned_date_sk
      ,t_time_sk sr_return_time_sk
      ,i_item_sk sr_item_sk
      ,c_customer_sk sr_customer_sk
      ,c_current_cdemo_sk sr_cdemo_sk
      ,c_current_hdemo_sk sr_hdemo_sk
      ,c_current_addr_sk sr_addr_sk
      ,s_store_sk sr_store_sk
      ,r_reason_sk sr_reason_sk
      ,sret_ticket_number sr_ticket_number
      ,sret_return_qty sr_return_quantity
      ,sret_return_amt sr_return_amt
      ,sret_return_tax sr_return_tax
      ,sret_return_amt + sret_return_tax sr_return_amt_inc_tax
      ,sret_return_fee sr_fee
      ,sret_return_ship_cost sr_return_ship_cost
      ,sret_refunded_cash sr_refunded_cash
      ,sret_reversed_charge sr_reversed_charde
      ,sret_store_credit sr_store_credit
      ,sret_return_amt+sret_return_tax+sret_return_fee
       -sret_refunded_cash-sret_reversed_charge-sret_store_credit sr_net_loss
from s_store_returns left outer join date_dim on (cast(sret_return_date as date) = d_date)
                       left outer join time_dim on (( cast(substring(sret_return_time,1,2) as integer)*3600
                                                     +cast(substring(sret_return_time,4,2) as integer)*60
                                                     +cast(substring(sret_return_time,7,2) as integer)) = t_time)
                     left outer join item on (sret_item_id = i_item_id)
                     left outer join customer on (sret_customer_id = c_customer_id)
                     left outer join store on (sret_store_id = s_store_id)
                     left outer join reason on (sret_reason_id = r_reason_id)
where i_rec_end_date is NULL
  and s_rec_end_date is NULL;"""


    nv_srv = """ create view nsrv as
select d_date_sk sr_returned_date_sk
      ,t_time_sk sr_return_time_sk
      ,i_item_sk sr_item_sk
      ,c_customer_sk sr_customer_sk
      ,c_current_cdemo_sk sr_cdemo_sk
      ,c_current_hdemo_sk sr_hdemo_sk
      ,c_current_addr_sk sr_addr_sk
      ,s_store_sk sr_store_sk
      ,r_reason_sk sr_reason_sk
      ,sret_ticket_number sr_ticket_number
      ,sret_return_qty sr_return_quantity
      ,sret_return_amt sr_return_amt
      ,sret_return_tax sr_return_tax
      ,sret_return_amt + sret_return_tax sr_return_amt_inc_tax
      ,sret_return_fee sr_fee
      ,sret_return_ship_cost sr_return_ship_cost
      ,sret_refunded_cash sr_refunded_cash
      ,sret_reversed_charge sr_reversed_charde
      ,sret_store_credit sr_store_credit
      ,sret_return_amt+sret_return_tax+sret_return_fee
       -sret_refunded_cash-sret_reversed_charge-sret_store_credit sr_net_loss
from s_store_returns left outer join date_dim on (cast(sret_return_date as date) = d_date)
                       left outer join time_dim on (( cast(substring(sret_return_time,1,2) as integer)*3600
                                                     +cast(substring(sret_return_time,4,2) as integer)*60
                                                     +cast(substring(sret_return_time,7,2) as integer)) = t_time)
                     left outer join item on (sret_item_id = i_item_id)
                     left outer join customer on (sret_customer_id = c_customer_id)
                     left outer join store on (sret_store_id = s_store_id)
                     left outer join reason on (sret_reason_id = r_reason_id)
where i_rec_end_date is NULL
  and s_rec_end_date is NULL;"""

    mv_ssv = """ create materialized view ssv BUILD IMMEDIATE 
 as
select  d_date_sk ss_sold_date_sk, 
        t_time_sk ss_sold_time_sk, 
        i_item_sk ss_item_sk, 
        c_customer_sk ss_customer_sk, 
        c_current_cdemo_sk ss_cdemo_sk, 
        c_current_hdemo_sk ss_hdemo_sk,
        c_current_addr_sk ss_addr_sk,
        s_store_sk ss_store_sk, 
        p_promo_sk ss_promo_sk,
        purc_purchase_id ss_ticket_number, 
        plin_quantity ss_quantity, 
        i_wholesale_cost ss_wholesale_cost, 
        i_current_price ss_list_price,
        plin_sale_price ss_sales_price,
        (i_current_price-plin_sale_price)*plin_quantity ss_ext_discount_amt,
        plin_sale_price * plin_quantity ss_ext_sales_price,
        i_wholesale_cost * plin_quantity ss_ext_wholesale_cost, 
        i_current_price * plin_quantity ss_ext_list_price, 
        i_current_price * s_tax_precentage ss_ext_tax, 
        plin_coupon_amt ss_coupon_amt,
        (plin_sale_price * plin_quantity)-plin_coupon_amt ss_net_paid,
        ((plin_sale_price * plin_quantity)-plin_coupon_amt)*(1+s_tax_precentage) ss_net_paid_inc_tax,
        ((plin_sale_price * plin_quantity)-plin_coupon_amt)-(plin_quantity*i_wholesale_cost) ss_net_profit
from    s_purchase left outer join customer on (purc_customer_id = c_customer_id) 
                     left outer join store on (purc_store_id = s_store_id)
                     left outer join date_dim on (cast(purc_purchase_date as date) = d_date)
                     left outer join time_dim on (PURC_PURCHASE_TIME = t_time),
        s_purchase_lineitem left outer join promotion on plin_promotion_id = p_promo_id
                           left outer join item on plin_item_id = i_item_id
where   purc_purchase_id = plin_purchase_id
    and i_rec_end_date is NULL
    and s_rec_end_date is NULL;"""

    nv_ssv= """ create view nssv as
select  d_date_sk ss_sold_date_sk, 
        t_time_sk ss_sold_time_sk, 
        i_item_sk ss_item_sk, 
        c_customer_sk ss_customer_sk, 
        c_current_cdemo_sk ss_cdemo_sk, 
        c_current_hdemo_sk ss_hdemo_sk,
        c_current_addr_sk ss_addr_sk,
        s_store_sk ss_store_sk, 
        p_promo_sk ss_promo_sk,
        purc_purchase_id ss_ticket_number, 
        plin_quantity ss_quantity, 
        i_wholesale_cost ss_wholesale_cost, 
        i_current_price ss_list_price,
        plin_sale_price ss_sales_price,
        (i_current_price-plin_sale_price)*plin_quantity ss_ext_discount_amt,
        plin_sale_price * plin_quantity ss_ext_sales_price,
        i_wholesale_cost * plin_quantity ss_ext_wholesale_cost, 
        i_current_price * plin_quantity ss_ext_list_price, 
        i_current_price * s_tax_precentage ss_ext_tax, 
        plin_coupon_amt ss_coupon_amt,
        (plin_sale_price * plin_quantity)-plin_coupon_amt ss_net_paid,
        ((plin_sale_price * plin_quantity)-plin_coupon_amt)*(1+s_tax_precentage) ss_net_paid_inc_tax,
        ((plin_sale_price * plin_quantity)-plin_coupon_amt)-(plin_quantity*i_wholesale_cost) ss_net_profit
from    s_purchase left outer join customer on (purc_customer_id = c_customer_id) 
                     left outer join store on (purc_store_id = s_store_id)
                     left outer join date_dim on (cast(purc_purchase_date as date) = d_date)
                     left outer join time_dim on (PURC_PURCHASE_TIME = t_time),
        s_purchase_lineitem left outer join promotion on plin_promotion_id = p_promo_id
                           left outer join item on plin_item_id = i_item_id
where   purc_purchase_id = plin_purchase_id
    and i_rec_end_date is NULL
    and s_rec_end_date is NULL;"""

    mv_storv = """create MATERIALIZED view storv BUILD IMMEDIATE 
 as
select s_store_sk
      ,stor_store_id s_store_id
      ,current_date s_rec_start_date
      ,cast(NULL as date) s_rec_end_date
      ,d1.d_date_sk s_closed_date_sk
      ,stor_name s_store_name
      ,stor_employees s_number_employees
      ,stor_floor_space s_floor_space
      ,stor_hours s_hours
      ,stor_store_manager s_manager
      ,stor_market_id s_market_id
      ,stor_geography_class s_geography_class
      ,s_market_desc
      ,stor_market_manager s_market_manager
      ,s_division_id
      ,s_division_name
      ,s_company_id
      ,s_company_name
      ,s_street_number
      ,s_street_name
      ,s_street_type
      ,s_suite_number
      ,s_city
      ,s_county
      ,s_state
      ,s_zip
      ,s_country
      ,s_gmt_offset
      ,stor_tax_percentage s_tax_percentage
from  s_store left outer join date_dim d1 on cast(stor_closed_date as date)= d1.d_date
     ,store
where  stor_store_id = s_store_id
   and s_rec_end_date is null; """

    nv_storv= """create view nstorv as
select s_store_sk
      ,stor_store_id s_store_id
      ,current_date s_rec_start_date
      ,cast(NULL as date) s_rec_end_date
      ,d1.d_date_sk s_closed_date_sk
      ,stor_name s_store_name
      ,stor_employees s_number_employees
      ,stor_floor_space s_floor_space
      ,stor_hours s_hours
      ,stor_store_manager s_manager
      ,stor_market_id s_market_id
      ,stor_geography_class s_geography_class
      ,s_market_desc
      ,stor_market_manager s_market_manager
      ,s_division_id
      ,s_division_name
      ,s_company_id
      ,s_company_name
      ,s_street_number
      ,s_street_name
      ,s_street_type
      ,s_suite_number
      ,s_city
      ,s_county
      ,s_state
      ,s_zip
      ,s_country
      ,s_gmt_offset
      ,stor_tax_percentage s_tax_percentage
from  s_store left outer join date_dim d1 on cast(stor_closed_date as date)= d1.d_date
     ,store
where  stor_store_id = s_store_id
   and s_rec_end_date is null;  """

    mv_websv = """create materialized view websv BUILD IMMEDIATE 
 as
select  web_site_sk
      ,wsit_web_site_id web_site_id
      ,current_date web_rec_start_date
      ,cast(null as date) web_rec_end_date
      ,wsit_site_name web_name
      ,d1.d_date_sk web_open_date_sk
      ,d2.d_date_sk web_close_date_sk
      ,wsit_site_class web_class
      ,wsit_site_manager web_manager
      ,web_mkt_id 
      ,web_mkt_class
      ,web_mkt_desc
      ,web_market_manager
      ,web_company_id
      ,web_company_name
      ,web_street_number 
      ,web_street_name
      ,web_street_type 
      ,web_suite_number
      ,web_city
      ,web_county 
      ,web_state
      ,web_zip
      ,web_country 
      ,web_gmt_offset
      ,wsit_tax_percentage web_tax_percentage
from  s_web_site left outer join date_dim d1 on (d1.d_date = wsit_open_date)
                   left outer join date_dim d2 on (d2.d_date = wsit_closed_date), 
      web_site
where web_site_id = wsit_web_site_id
  and web_rec_end_date is null; """


    nv_websv = """create view nwebsv as
select  web_site_sk
      ,wsit_web_site_id web_site_id
      ,current_date web_rec_start_date
      ,cast(null as date) web_rec_end_date
      ,wsit_site_name web_name
      ,d1.d_date_sk web_open_date_sk
      ,d2.d_date_sk web_close_date_sk
      ,wsit_site_class web_class
      ,wsit_site_manager web_manager
      ,web_mkt_id 
      ,web_mkt_class
      ,web_mkt_desc
      ,web_market_manager
      ,web_company_id
      ,web_company_name
      ,web_street_number 
      ,web_street_name
      ,web_street_type 
      ,web_suite_number
      ,web_city
      ,web_county 
      ,web_state
      ,web_zip
      ,web_country 
      ,web_gmt_offset
      ,wsit_tax_percentage web_tax_percentage
from  s_web_site left outer join date_dim d1 on (d1.d_date = wsit_open_date)
                   left outer join date_dim d2 on (d2.d_date = wsit_closed_date), 
      web_site
where web_site_id = wsit_web_site_id
  and web_rec_end_date is null; """


    mv_webv = """create materialized view webv BUILD IMMEDIATE 
 as
select wp_web_page_sk,
      wpag_web_page_id wp_web_page_id
      ,current_date wp_rec_start_date
      ,cast(null as date) wp_rec_end_date
      ,d1.d_date_sk wp_creation_date_sk
      ,d2.d_date_sk wp_access_date_sk
      ,wpag_autogen_flag wp_autogen_flag
      ,wpag_url wp_url
      ,wpag_type wp_type 
      ,wpag_char_cnt wp_char_count
      ,wpag_link_cnt wp_link_count
      ,wpag_image_cnt wp_image_count
      ,wpag_max_ad_cnt wp_max_ad_count
from   web_page, s_web_page left outer join date_dim d1 on wpag_create_date  = d1.d_date
                     left outer join date_dim d2 on wpag_access_date  = d2.d_date; """

    nv_webv = """create view nwebv as
select wp_web_page_sk,
      wpag_web_page_id wp_web_page_id
      ,current_date wp_rec_start_date
      ,cast(null as date) wp_rec_end_date
      ,d1.d_date_sk wp_creation_date_sk
      ,d2.d_date_sk wp_access_date_sk
      ,wpag_autogen_flag wp_autogen_flag
      ,wpag_url wp_url
      ,wpag_type wp_type 
      ,wpag_char_cnt wp_char_count
      ,wpag_link_cnt wp_link_count
      ,wpag_image_cnt wp_image_count
      ,wpag_max_ad_cnt wp_max_ad_count
from   web_page, s_web_page left outer join date_dim d1 on wpag_create_date  = d1.d_date
                     left outer join date_dim d2 on wpag_access_date  = d2.d_date;"""

    mv_wrhsv = """create MATERIALIZED view wrhsv BUILD IMMEDIATE 
 as
select  wrhs_warehouse_id w_warehouse_id
       ,wrhs_warehouse_desc w_warehouse_name
       ,wrhs_warehouse_sq_ft w_warehouse_sq_ft
       ,w_street_number
       ,w_street_name
       ,w_street_type
       ,w_suite_number
       ,w_city
       ,w_county
       ,w_state
       ,w_zip
       ,w_country
       ,w_gmt_offset
from    s_warehouse,
        warehouse
where   wrhs_warehouse_id = w_warehouse_id;"""

    nv_wrhsv = """create view nwrhsv as
select  wrhs_warehouse_id w_warehouse_id
       ,wrhs_warehouse_desc w_warehouse_name
       ,wrhs_warehouse_sq_ft w_warehouse_sq_ft
       ,w_street_number
       ,w_street_name
       ,w_street_type
       ,w_suite_number
       ,w_city
       ,w_county
       ,w_state
       ,w_zip
       ,w_country
       ,w_gmt_offset
from    s_warehouse,
        warehouse
where   wrhs_warehouse_id = w_warehouse_id;"""

    mv_wrv = """create materialized view wrv BUILD IMMEDIATE 
 as
select d_date_sk wr_return_date_sk
      ,t_time_sk wr_return_time_sk
      ,i_item_sk wr_item_sk
      ,c1.c_customer_sk wr_refunded_customer_sk
      ,c1.c_current_cdemo_sk wr_refunded_cdemo_sk
      ,c1.c_current_hdemo_sk wr_refunded_hdemo_sk
      ,c1.c_current_addr_sk wr_refunded_addr_sk
      ,c2.c_customer_sk wr_returning_customer_sk
      ,c2.c_current_cdemo_sk wr_returning_cdemo_sk
      ,c2.c_current_hdemo_sk wr_returning_hdemo_sk
      ,c2.c_current_addr_sk wr_returing_addr_sk
      ,wp_web_page_sk wr_web_page_sk 
      ,r_reason_sk wr_reason_sk
      ,wret_order_id wr_order_number
      ,wret_return_qty wr_return_quantity
      ,wret_return_amt wr_return_amt
      ,wret_return_tax wr_return_tax
      ,wret_return_amt + wret_return_tax as wr_return_amt_inc_tax
      ,wret_return_fee wr_fee
      ,wret_return_ship_cost wr_return_ship_cost
      ,wret_refunded_cash wr_refunded_cash
      ,wret_reversed_charge wr_reversed_charde
      ,wret_account_credit wr_account_credit
      ,wret_return_amt+wret_return_tax+wret_return_fee
       -wret_refunded_cash-wret_reversed_charge-wret_account_credit wr_net_loss
from s_web_returns left outer join date_dim on (cast(wret_return_date as date) = d_date)
                   left outer join time_dim on (( cast(substring(wret_return_time,1,2) as integer)*3600
                                                 +cast(substring(wret_return_time,4,2) as integer)*60
                                                 +cast(substring(wret_return_time,7,2) as integer)) = t_time)
                   left outer join item on (wret_item_id = i_item_id)
                   left outer join customer c1 on (wret_return_customer_id = c1.c_customer_id)
                   left outer join customer c2 on (wret_refund_customer_id = c2.c_customer_id)
                   left outer join reason on (wret_reason_id = r_reason_id)
                   left outer join web_page on (wret_web_site_id = WP_WEB_PAGE_id)
where i_rec_end_date is NULL
  and wp_rec_end_date is NULL;"""

    nv_wrv = """create view nwrv as
select d_date_sk wr_return_date_sk
      ,t_time_sk wr_return_time_sk
      ,i_item_sk wr_item_sk
      ,c1.c_customer_sk wr_refunded_customer_sk
      ,c1.c_current_cdemo_sk wr_refunded_cdemo_sk
      ,c1.c_current_hdemo_sk wr_refunded_hdemo_sk
      ,c1.c_current_addr_sk wr_refunded_addr_sk
      ,c2.c_customer_sk wr_returning_customer_sk
      ,c2.c_current_cdemo_sk wr_returning_cdemo_sk
      ,c2.c_current_hdemo_sk wr_returning_hdemo_sk
      ,c2.c_current_addr_sk wr_returing_addr_sk
      ,wp_web_page_sk wr_web_page_sk 
      ,r_reason_sk wr_reason_sk
      ,wret_order_id wr_order_number
      ,wret_return_qty wr_return_quantity
      ,wret_return_amt wr_return_amt
      ,wret_return_tax wr_return_tax
      ,wret_return_amt + wret_return_tax as wr_return_amt_inc_tax
      ,wret_return_fee wr_fee
      ,wret_return_ship_cost wr_return_ship_cost
      ,wret_refunded_cash wr_refunded_cash
      ,wret_reversed_charge wr_reversed_charde
      ,wret_account_credit wr_account_credit
      ,wret_return_amt+wret_return_tax+wret_return_fee
       -wret_refunded_cash-wret_reversed_charge-wret_account_credit wr_net_loss
from s_web_returns left outer join date_dim on (cast(wret_return_date as date) = d_date)
                   left outer join time_dim on (( cast(substring(wret_return_time,1,2) as integer)*3600
                                                 +cast(substring(wret_return_time,4,2) as integer)*60
                                                 +cast(substring(wret_return_time,7,2) as integer)) = t_time)
                   left outer join item on (wret_item_id = i_item_id)
                   left outer join customer c1 on (wret_return_customer_id = c1.c_customer_id)
                   left outer join customer c2 on (wret_refund_customer_id = c2.c_customer_id)
                   left outer join reason on (wret_reason_id = r_reason_id)
                   left outer join web_page on (wret_web_site_id = WP_WEB_PAGE_id)
where i_rec_end_date is NULL
  and wp_rec_end_date is NULL;"""

    mv_wsv = """create materialized view wsv BUILD IMMEDIATE 
 as
select  d1.d_date_sk ws_sold_date_sk, 
        t_time_sk ws_sold_time_sk, 
        d2.d_date_sk ws_ship_date_sk,
        i_item_sk ws_item_sk, 
        c1.c_customer_sk ws_bill_customer_sk, 
        c1.c_current_cdemo_sk ws_bill_cdemo_sk, 
        c1.c_current_hdemo_sk ws_bill_hdemo_sk,
        c1.c_current_addr_sk ws_bill_addr_sk,
        c2.c_customer_sk ws_ship_customer_sk,
        c2.c_current_cdemo_sk ws_ship_cdemo_sk,
        c2.c_current_hdemo_sk ws_ship_hdemo_sk,
        c2.c_current_addr_sk ws_ship_addr_sk,
        web_site_sk ws_web_page_sk,
        wp_web_page_sk ws_web_site_s,
        sm_ship_mode_sk ws_ship_mode_sk,
        w_warehouse_sk ws_warehouse_sk,
        p_promo_sk ws_promo_sk,
        word_order_id ws_order_number, 
        wlin_quantity ws_quantity, 
        i_wholesale_cost ws_wholesale_cost, 
        i_current_price ws_list_price,
        wlin_sales_price ws_sales_price,
        (i_current_price-wlin_sales_price)*wlin_quantity ws_ext_discount_amt,
        wlin_sales_price * wlin_quantity ws_ext_sales_price,
        i_wholesale_cost * wlin_quantity ws_ext_wholesale_cost, 
        i_current_price * wlin_quantity ws_ext_list_price, 
        i_current_price * web_tax_percentage ws_ext_tax,  
        wlin_coupon_amt ws_coupon_amt,
        0 WS_EXT_SHIP_COST,
        (wlin_sales_price * wlin_quantity)-wlin_coupon_amt ws_net_paid,
        ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)*(1+web_tax_percentage) ws_net_paid_inc_tax,
        ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)-(wlin_quantity*i_wholesale_cost) WS_NET_PAID_INC_SHIP,
        (wlin_sales_price * wlin_quantity)-wlin_coupon_amt + (wlin_ship_cost * wlin_quantity)
        + i_current_price * web_tax_percentage WS_NET_PAID_INC_SHIP_TAX,
        ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)-(i_wholesale_cost * wlin_quantity) WS_NET_PROFIT
from    s_web_order left outer join date_dim d1 on (cast(word_order_date as date) =  d1.d_date)
                    left outer join time_dim on (word_order_time = t_time)
                    left outer join customer c1 on (word_bill_customer_id = c1.c_customer_id)
                    left outer join customer c2 on (word_ship_customer_id = c2.c_customer_id)
                    left outer join web_site on (word_web_site_id = web_site_id)
                    left outer join ship_mode on (word_ship_mode_id = sm_ship_mode_id), 
        s_web_order_lineitem left outer join date_dim d2 on (cast(wlin_ship_date as date) = d2.d_date)
                             left outer join item on (wlin_item_id = i_item_id)
                             left outer join web_page on (wlin_web_page_id = wp_web_page_id)
                             left outer join warehouse on (wlin_warehouse_id = w_warehouse_id)
                             left outer join promotion on (wlin_promotion_id = p_promo_id)
where   word_order_id = wlin_order_id
    and i_rec_end_date is NULL 
    and web_rec_end_date is null
    and wp_rec_end_date is null; """


    nv_wsv = """create view Nwsv as
select  d1.d_date_sk ws_sold_date_sk, 
        t_time_sk ws_sold_time_sk, 
        d2.d_date_sk ws_ship_date_sk,
        i_item_sk ws_item_sk, 
        c1.c_customer_sk ws_bill_customer_sk, 
        c1.c_current_cdemo_sk ws_bill_cdemo_sk, 
        c1.c_current_hdemo_sk ws_bill_hdemo_sk,
        c1.c_current_addr_sk ws_bill_addr_sk,
        c2.c_customer_sk ws_ship_customer_sk,
        c2.c_current_cdemo_sk ws_ship_cdemo_sk,
        c2.c_current_hdemo_sk ws_ship_hdemo_sk,
        c2.c_current_addr_sk ws_ship_addr_sk,
        web_site_sk ws_web_page_sk,
        wp_web_page_sk ws_web_site_s,
        sm_ship_mode_sk ws_ship_mode_sk,
        w_warehouse_sk ws_warehouse_sk,
        p_promo_sk ws_promo_sk,
        word_order_id ws_order_number, 
        wlin_quantity ws_quantity, 
        i_wholesale_cost ws_wholesale_cost, 
        i_current_price ws_list_price,
        wlin_sales_price ws_sales_price,
        (i_current_price-wlin_sales_price)*wlin_quantity ws_ext_discount_amt,
        wlin_sales_price * wlin_quantity ws_ext_sales_price,
        i_wholesale_cost * wlin_quantity ws_ext_wholesale_cost, 
        i_current_price * wlin_quantity ws_ext_list_price, 
        i_current_price * web_tax_percentage ws_ext_tax,  
        wlin_coupon_amt ws_coupon_amt,
        0 WS_EXT_SHIP_COST,
        (wlin_sales_price * wlin_quantity)-wlin_coupon_amt ws_net_paid,
        ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)*(1+web_tax_percentage) ws_net_paid_inc_tax,
        ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)-(wlin_quantity*i_wholesale_cost) WS_NET_PAID_INC_SHIP,
        (wlin_sales_price * wlin_quantity)-wlin_coupon_amt + (wlin_ship_cost * wlin_quantity)
        + i_current_price * web_tax_percentage WS_NET_PAID_INC_SHIP_TAX,
        ((wlin_sales_price * wlin_quantity)-wlin_coupon_amt)-(i_wholesale_cost * wlin_quantity) WS_NET_PROFIT
from    s_web_order left outer join date_dim d1 on (cast(word_order_date as date) =  d1.d_date)
                    left outer join time_dim on (word_order_time = t_time)
                    left outer join customer c1 on (word_bill_customer_id = c1.c_customer_id)
                    left outer join customer c2 on (word_ship_customer_id = c2.c_customer_id)
                    left outer join web_site on (word_web_site_id = web_site_id)
                    left outer join ship_mode on (word_ship_mode_id = sm_ship_mode_id), 
        s_web_order_lineitem left outer join date_dim d2 on (cast(wlin_ship_date as date) = d2.d_date)
                             left outer join item on (wlin_item_id = i_item_id)
                             left outer join web_page on (wlin_web_page_id = wp_web_page_id)
                             left outer join warehouse on (wlin_warehouse_id = w_warehouse_id)
                             left outer join promotion on (wlin_promotion_id = p_promo_id)
where   word_order_id = wlin_order_id
    and i_rec_end_date is NULL 
    and web_rec_end_date is null
    and wp_rec_end_date is null; """

    cursor.execute(mv_crv)
    cursor.execute(nv_crv)
    cursor.execute(mv_csv)
    cursor.execute(nv_csv)
    cursor.execute(mv_itemv)
    cursor.execute(nv_itemv)
    cursor.execute(mv_promv)
    cursor.execute(nv_promv)
    cursor.execute(mv_ccv)
    cursor.execute(nv_ccv)
    cursor.execute(mv_srv)
    cursor.execute(nv_srv)
    cursor.execute(mv_ssv)
    cursor.execute(nv_ssv)
    cursor.execute(mv_storv)
    cursor.execute(nv_storv)
    cursor.execute(mv_websv)
    cursor.execute(nv_websv)
    cursor.execute(mv_webv)
    cursor.execute(nv_webv)
    cursor.execute(mv_wrhsv)
    cursor.execute(nv_wrhsv)
    cursor.execute(mv_wrv)
    cursor.execute(nv_wrv)
    cursor.execute(mv_wsv)
    cursor.execute(nv_wsv)

    #Storing sql queries
    mv_sql0 = """insert into catalog_returns select * from crv;"""
    nv_sql0 = """insert into catalog_returns select * from ncrv;"""
    mv_sql1 = """insert into catalog_sales select * from csv"""
    nv_sql1 = """insert into catalog_sales select * from ncsv """
    mv_sql2 = """insert into item select * from itemv """
    nv_sql2 = """insert into item select * from nitemv """
    mv_sql3 = """select count(*) from promv s,promotion d where s.P_PROMO_ID=d.P_PROMO_ID; """
    nv_sql3 = """select count(*) from npromv s,promotion d where s.P_PROMO_ID=d.P_PROMO_ID; """
    mv_sql4 = """insert into call_center select * from ccv; """
    nv_sql4 = """insert into call_center select * from nccv; """
    mv_sql5 = """insert into store_returns select * from srv """
    nv_sql5 = """insert into store_returns select * from nsrv """
    mv_sql6 = """insert into store_sales select * from ssv """
    nv_sql6 = """insert into store_sales select * from nssv """
    mv_sql7 = """insert into store select * from storv """
    nv_sql7 = """insert into store select * from nstorv """
    mv_sql8 = """insert into web_site select * from websv """
    nv_sql8 = """insert into web_site select * from nwebsv """
    mv_sql9 = """insert into web_page select * from webv """
    nv_sql9 = """ insert into web_page select * from nwebv"""
    mv_sql10 = """select count(*) from wrhsv; """
    nv_sql10 = """select count(*) from nwrhsv; """
    mv_sql11 = """insert into web_returns select * from wrv """
    nv_sql11 = """ insert into web_returns select * from nwrv"""
    mv_sql12 = """insert into web_sales select * from wsv """
    nv_sql12 = """insert into web_sales select * from wsv """


    a = datetime.datetime.now()
    cursor.execute(mv_sql0)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized crv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql0)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized crv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql1)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized csv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql1)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized csv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql2)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized itemv ",int(delta.total_seconds()*10000))
    
    a = datetime.datetime.now()
    cursor.execute(nv_sql2)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized itemv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql3)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized promv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql3)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized promv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql4)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized ccv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql4)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized ccv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql5)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized srv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql5)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized srv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql6)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized ssv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql6)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized ssv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql7)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized storv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql7)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized storv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql8)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized websv ",int(delta.total_seconds()*10000))


    a = datetime.datetime.now()
    cursor.execute(nv_sql8)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized websv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql9)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized webv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql9)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized webv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql10)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized wrhsv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql10)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized wrhsv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql11)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized wrv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(nv_sql11)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized wrv ",int(delta.total_seconds()*10000))

    a = datetime.datetime.now()
    cursor.execute(mv_sql12)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for materialized wsv ",int(delta.total_seconds()*10000))


    a = datetime.datetime.now()
    cursor.execute(nv_sql12)
    b = datetime.datetime.now()
    delta = b-a
    print(delta)
    print ("Total time taken for non-materialized wsv ",int(delta.total_seconds()*10000))



print("1: Postgres")
print("2: SQL Server")
print("3: SQL Anywhere")
print("4: Oracle sql")
print("Please enter which db would you like to use?")
db_type = int(input())

if db_type == 1:
    postgres()
if db_type == 2:
    SQLServer()
if db_type == 3:
    SQLAnywhere()
if db_type == 4:
    OracleSQL()






