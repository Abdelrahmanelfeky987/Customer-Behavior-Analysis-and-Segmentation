-- Q1 Exploring Dataset
 
--a)    This query calculates total  per month, computes the percentage increase in sales compared to the previous month to compare sales trends over time .

with month_sales as (
select distinct
    to_char(to_date(invoicedate, 'MM/DD/YYYY HH24:MI'), 'fmMonth YYYY') as month_,
    sum(price * quantity) over (partition by to_char(to_date(invoicedate, 'MM/DD/YYYY HH24:MI'), 'fmMonth YYYY')) as Total_sales_per_month
from
    tableRetail
    ),
lag as (
    select  month_,Total_sales_per_month,  lag(Total_sales_per_month, 1) over(order by to_date(month_, 'Mon-yyyy')) prev_sales
    from month_sales
),
percent_ as (
    select month_, Total_sales_per_month , round(100*(Total_sales_per_month - prev_sales)/prev_sales,2) "increase_percentage"
    from lag
    order by to_date(month_, 'Mon-yyyy') 
)
select * from percent_;



-- b)    This query retrieves information about the top 10 best-selling products. It calculates the total quantity sold, total sales amount, and assigns a sales rank based on the total sales amount.


select stockcode,  Total_quantity , Total_sales , Sales_rank
from (
select stockcode , sum(quantity) as Total_quantity ,
                         round(sum(price * quantity)) as Total_sales
                         , dense_rank() over (order by sum(price * quantity) desc) as Sales_rank
from tableRetail
group by stockcode) 
where Sales_rank <= 10 
order by Sales_rank ;



--c)    This query calculate the top 10 customers with the highest total sales and presents , the number of invoices for each customer 

select customer_id ,total_sales_per_customer , Number_of_invoices ,Sales_rank
from (
select  customer_id , sum(quantity * price ) as  total_sales_per_customer ,
                                        count(invoice)  as Number_of_invoices ,
                                        dense_rank() over (order by sum(price * quantity) desc) as Sales_rank
from tableRetail
group by customer_id 
)
where sales_rank <= 10
order by Sales_rank ;

-- d)    This query provides insights into the most profitable product for each month and their respective total profits

select distinct country from tableRetail;

with product_month as (
select stockcode , (price*quantity) as profit , to_char(to_date(invoicedate, 'MM/DD/YYYY HH24:MI'), 'fmMonth YYYY') as month_
from tableRetail
),
most_purchased as (
    select month_ ,  stockcode , dense_rank() over(partition by month_ order by sum(profit) desc) as Sales_rank
                                            ,sum(profit) as total_profit
    from  product_month
    group by month_ , stockcode
    ),
top_rank as (
    select month_ , stockcode , total_profit , Sales_rank 
    from most_purchased
    where Sales_rank =1
    )
select * from top_rank
order by to_date('01 ' || month_, 'DD Month YYYY');


--e)    This query helps identify the top-performing invoices based on their total profit and presents information about cust_id of it’s owner 

select invoice , customer_id , profit , invoice_rank
from (
select invoice , customer_id , sum(price * quantity) as profit ,
                                           dense_rank() over ( order by sum(price*quantity) desc ) as invoice_rank 
from tableRetail 
group by invoice , customer_id
)
where   invoice_rank <= 10;



------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------- Q2
with table_1 as (
select  distinct customer_id ,round(maxdate - max(to_date(invoicedate, 'mm/dd/yyyy hh24:mi'))  over(partition by customer_id)) as  recency 
                                        ,count(distinct invoice) over(partition by customer_id) as  frequency
                                        ,round(sum(price * quantity) over(partition by customer_id),2) as monetary                                           
from tableretail, (select max(to_date(invoicedate, 'mm/dd/yyyy hh24:mi')) maxdate from tableretail)
),
r_fm_scores as (
select customer_id ,recency , frequency , monetary , ntile(5) over(order by recency desc) as r_score
                                                                           ,ntile(5) over(order by ((frequency + monetary) / 2) ) as fm_score 
from table_1
),
cust_segment as (
select customer_id ,recency , frequency , monetary , r_score , fm_score ,
 CASE
          WHEN  (r_score= 5 AND  fm_score= 5) OR 
                     (r_score= 5 AND  fm_score= 4) OR
                     (r_score= 4 AND  fm_score= 5) THEN 'Champions'
                     
          WHEN (r_score = 5 AND fm_score = 3) OR
                    (r_score = 4 AND fm_score = 4) OR
                    (r_score = 3 AND fm_score = 5) OR
                    (r_score = 3 AND fm_score = 4) THEN 'Loyal Customers'
                    
          WHEN (r_score = 5 AND fm_score = 2) OR
                    (r_score = 4 AND fm_score = 2) OR
                    (r_score = 3 AND fm_score = 3) OR
                    (r_score = 4 AND fm_score = 3) THEN 'Potential Loyalists'
                    
          WHEN (r_score = 5 AND fm_score = 1) THEN 'Recent Customers'
          
          WHEN (r_score = 4 AND fm_score = 1) OR
                    (r_score = 3 AND fm_score = 1) THEN 'Promising'
                    
          WHEN (r_score = 3 AND fm_score = 2) OR
                    (r_score = 2 AND fm_score = 3) OR
                    (r_score = 2 AND fm_score = 2) THEN 'Customers Needing Attention'
                    
          WHEN (r_score = 2 AND fm_score = 5) OR
                     (r_score = 2 AND fm_score = 4) OR
                     (r_score = 1 AND fm_score = 3) THEN 'At Risk'
                    
          WHEN (r_score = 1 AND fm_score = 5) OR
                     (r_score = 1 AND fm_score = 4) THEN 'Can not lose them'
                    
          WHEN (r_score = 1 AND fm_score = 2) THEN 'Hibernating'
          
          WHEN (r_score = 1 AND fm_score = 1) THEN 'Lost'
          
          ELSE 'Unclassified'
        END AS customer_segment
from r_fm_scores)
select * from cust_segment;


------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------- Q3 
--------a)    Maximum consecutive purchase days per customer:

with difference as (
select cust_id, calendar_dt
                  , lead(calendar_dt) over (partition by cust_id order by calendar_dt) as next_days
                  , calendar_dt - row_number() over (partition by cust_id order by calendar_dt) as difference
from customer
),
calculate_days as (
select cust_id , calendar_dt , next_days , difference ,  count(*) over (partition by cust_id, difference order by calendar_dt) as days_no
from difference 
)
select cust_id , max(days_no) as Max_consecutive_days 
from calculate_days
group by cust_id

-----b)    Average Days or Transactions did the customer take to reach 250 le total sales

with customer_days as (
select cust_id , calendar_dt , sum(amt_le) over(partition by cust_id order by calendar_dt rows between unbounded preceding and current row) as sum_amt
                                         ,count(*) over(partition by cust_id order by calendar_dt ) as total_days
from customer
where amt_le != 0
),
days_till_250 as (
select cust_id  , (min(total_days)) days_to_250
from customer_days
where sum_amt >= 250
group by cust_id
) 
select round(avg(days_to_250)) from days_till_250;
