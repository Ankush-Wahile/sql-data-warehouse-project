/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/


--TIP--> After joining in gold layer ,after joining the tables, check if any duplicates were introduced by the join logic
/*

select cst_id ,count(*) FROM
(select 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON 		  ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON 		  ci.cst_key=la.cid) t GROUP BY cst_id HAVING count(*) >1;
--
*/
--NULLs often comes from joined tables! NULL will appear if SQL finds no match

--Suppose we have two columns like gender and we are getting some two values which are not comman for some of the rows in the two columns
-- so we need to ask the our seniors Which source is the master for this values?
-- Suppose the seniors says --> The Master Source Of Customer Data is CRM
-- so we need to select(provide the correct data for gender column) the gender column from the CRM table(sorces)


--Rename columns to friendly meangful names
--> Follow the RULES
--> 1) Naming Conventions,2) Language ,3)Avoid Reserved Words

-->Sort the columns into logical groups to improve readability


--DIMENSIONS VS FACT

--Dimensions--> Decsriptive inforamtions only
--> If you are creating  a new dimensions you always need a primary key
--> Surrogate Keys--> System Generated Unique identifier assigned to each record in the table.
-->(It is not a business key)--> we only used it in order to connect our data model
-->  In this way we have more control on data model and we don't have to depend always on source system

--> there are multiple ways to generate surrogate keys
-->1) DDL -based  generation
-->2) Query based using Window function(Row_Number)



-->>CREATE CUSTOMER DIMENSION
DROP VIEW IF EXISTS gold.dim_customers;
CREATE VIEW gold.dim_customers AS 
select 
	ROW_NUMBER() OVER (ORDER BY cst_id) as customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status as marital_status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr  --CRM is the master for gender Info
			ELSE COALESCE(ca.gen ,'n/a')
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON 		  ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON 		  ci.cst_key=la.cid;





-->> QUALITY CHECK OF GOLD TABLE(VIEW)
select distinct gender from gold.dim_customers



------> CREATE DIMENSION PRODUCTS

DROP VIEW IF EXISTS gold.dim_products;
CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt,pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS costing,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id=pc.id
WHERE prd_end_dt IS NULL -- Filter out all historical data





--- CREATE FACT SALES
DROP VIEW IF EXISTS gold.fact_sales;
CREATE VIEW gold.fact_sales AS
SELECT
	sd.sls_ord_num AS order_number,
	pr.product_key,
	cu.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS shipping_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id=cu.customer_id


--> Use the dimension's surrogate keys instead of ID's to easily connect facts with dimensions


--> Now check if all the dimension tables can successfully join the fact table
SELECT * from gold.fact_sales f
LEFT JOIN gold.dim_customers c
on c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key=f.product_key
WHERE p.product_key IS NULL

--> IN star schema the relationship between fact and dimension is one to many(1:N) ,
--> fact side is many and dimension side is one
--> ONE(mandatory) TO MANY option(customer must exist in the dimension table)
--> many(optional)
--> 1.customer's havent placed any orders yet
--> 2.Customer who have placed only one order
--> 3.Customer who have placed multiple orders
--> ONE(mandatory) TO MANY option(customer must exist in the dimension table)
--> many(optional)
--> 1.customer's havent placed any orders yet
--> 2.Customer who have placed only one order
--> 3.Customer who have placed multiple orders
