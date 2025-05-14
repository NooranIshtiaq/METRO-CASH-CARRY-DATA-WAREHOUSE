use Metro_Dwh_Project; 
-- Query 3
select
    product.STORE_ID,product.STORE_NAME,product.SUPPLIER_ID,product.SUPPLIER_NAME,product.PRODUCT_NAME,
    SUM( salesfact.QUANTITY * product.PRODUCT_PRICE) AS Total_SALES_Contributed 
from Metro_Sales_Fact salesfact
inner join PRODUCT_DIMENSION product  ON  (salesfact.PRODUCT_ID = product.PRODUCT_ID)         
group by
    product.STORE_ID, product.STORE_NAME,                  
    product.SUPPLIER_ID, product.SUPPLIER_NAME,           
    product. PRODUCT_NAME                                 
order by product.STORE_ID, product.SUPPLIER_ID, product.PRODUCT_NAME;  

-- Query 10
CREATE VIEW quaterly_sales_of_store AS
select p.STORE_ID,p.STORE_NAME,t.YEAR,t.QUARTER,SUM(salefact.SALE) AS total_sales          
from Metro_Sales_Fact salefact
inner join PRODUCT_DIMENSION p ON salefact.PRODUCT_ID = p.PRODUCT_ID  
inner join TIME_DIMENSION t ON salefact.TIME_ID = t.TIME_ID           
group by p.STORE_ID, p.STORE_NAME, t.YEAR, t.QUARTER         
order by p.STORE_NAME, t.YEAR, t.QUARTER;                     

select * from quaterly_sales_of_store;

-- Query 7
select P.STORE_NAME,P.SUPPLIER_NAME,P.PRODUCT_NAME,T.YEAR,SUM(SF.SALE) AS TOTAL_REVENUE         
from METRO_SALES_FACT SF
inner join PRODUCT_DIMENSION P ON SF.PRODUCT_ID = P.PRODUCT_ID    
inner join    TIME_DIMENSION T ON SF.TIME_ID = T.TIME_ID            
group by P.STORE_NAME, P.SUPPLIER_NAME, P.PRODUCT_NAME, T.YEAR   
with ROLLUP                           
order by P.STORE_NAME, P.SUPPLIER_NAME, P.PRODUCT_NAME, T.YEAR;  
 
-- Query 1 

SELECT P.PRODUCT_NAME,T.MONTH,
    SUM(CASE WHEN T.DAY_OF_WEEK BETWEEN 1 AND 5 THEN sales_f.SALE ELSE 0 END) AS total_weekday_reveue,
    SUM(CASE WHEN T.DAY_OF_WEEK IN (6, 7) THEN sales_f.SALE ELSE 0 END) AS total_weekend_revenue, SUM(sales_f.SALE) AS Total_revenue
from Metro_Sales_Fact sales_f
inner join PRODUCT_DIMENSION P 
on sales_f.PRODUCT_ID = P.PRODUCT_ID
inner join TIME_DIMENSION T ON sales_f.TIME_ID = T.TIME_ID
where T.YEAR = 2019 
group by P.PRODUCT_NAME, T.MONTH
order by TOTAL_REVENUE DESC  
limit 50;

-- query 2 

select P.STORE_NAME,T.QUARTER,SUM(SF.SALE) AS TOTAL_REVENUE
from  
    Metro_Sales_Fact sales_f
inner join PRODUCT_DIMENSION P ON sales_f.PRODUCT_ID = P.PRODUCT_ID
inner join TIME_DIMENSION T ON sales_f.TIME_ID = T.TIME_ID
where T.YEAR = 2019 
group by P.STORE_NAME, T.QUARTER
order by P.STORE_NAME, T.QUARTER;

-- query 4
SELECT Pr.PRODUCT_NAME,
    CASE
        WHEN MONTH(T.ORDERDATE) IN (3, 4, 5) THEN 'Spring'
        WHEN MONTH(T.ORDERDATE) IN (6, 7, 8) THEN 'Summer'
        WHEN MONTH(T.ORDERDATE) IN (9, 10, 11) THEN 'Fall'
        WHEN MONTH(T.ORDERDATE) IN (12, 1, 2) THEN 'Winter'
    END AS SEASON,
    SUM(sales_f.SALE) AS TOTAL_SALES
FROM METRO_SALES_FACT sales_f
inner join PRODUCT_DIMENSION  Pr ON sales_f.PRODUCT_ID =  Pr.PRODUCT_ID
inner join TIME_DIMENSION T ON sales_f.TIME_ID = T.TIME_ID
group by Pr.PRODUCT_NAME, SEASON
order by Pr.PRODUCT_NAME, SEASON;


-- query 8
select
    Pr.PRODUCT_NAME,
    SUM(CASE WHEN MONTH(T.ORDERDATE) BETWEEN 1 AND 6 THEN SF.SALE ELSE 0 END) AS H1_REVENUE,
    SUM(CASE WHEN MONTH(T.ORDERDATE) BETWEEN 1 AND 6 THEN SF.QUANTITY ELSE 0 END) AS H1_QUANTITY,
    SUM(CASE WHEN MONTH(T.ORDERDATE) BETWEEN 7 AND 12 THEN SF.SALE ELSE 0 END) AS H2_REVENUE,
    SUM(CASE WHEN MONTH(T.ORDERDATE) BETWEEN 7 AND 12 THEN SF.QUANTITY ELSE 0 END) AS H2_QUANTITY,
    SUM(SF.SALE) AS YEARLY_REVENUE,
    SUM(SF.QUANTITY) AS YEARLY_QUANTITY
FROM 
    METRO_SALES_FACT SF
inner join PRODUCT_DIMENSION Pr ON SF.PRODUCT_ID = Pr.PRODUCT_ID
inner join TIME_DIMENSION T ON SF.TIME_ID = T.TIME_ID
group by Pr.PRODUCT_NAME
order by Pr.PRODUCT_NAME;





