/*
====================================================================
DDL Script: Create gold Views
====================================================================

Script Purpose:
	This Script create Views for the gold layer in the Data wareHouse, droping existing Views if they exit.
	The gold layer represent the final Dimension and Fact tables (Star Schema). 

	Each Viewss performs transformation and combine tables from the silver layer to produce a cleas,
	enriched and Business-Ready dataset.

Usage:
	These Views can be queried directly for data analytics and reporting
	
*/

IF OBJECT_ID ('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY cus.cst_id) AS customer_key,
	cus.cst_id AS customer_id,
	cus.cst_key AS customer_number,
	cus.cst_firstname AS first_name,
	cus.cst_lastname AS last_name,
	loc.cntry AS country,
	CASE WHEN cus.cst_gndr != 'Unavailable' THEN cus.cst_gndr
		ELSE COALESCE(cos.gen, 'Unavailable')
		END gender,
	cus.cst_marital_status AS marital_status,
	cos.bdate AS birth_date,
	cus.cst_create_date AS create_date
FROM silver.crm_cust_info cus
LEFT JOIN silver.erp_cust_az12 cos
ON cus.cst_key = cos.cid
LEFT JOIN silver.erp_loc_a101 loc
ON cus.cst_key = loc.cid

GO


IF OBJECT_ID ('gold.dim_products', 'V') IS NOT NULL
	DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY prd.prd_start_dt, prd.prd_key) AS product_key,
	prd.prd_id AS product_id,
	prd.prd_key AS product_number,
	prd.prd_nm AS product_name,
	prd.prd_cat AS category_id,
	cat.cat AS category,
	cat.subcat AS subcategory,
	cat.maintenance,
	prd.prd_line AS product_line,
	prd.prd_cost AS product_cost,
	prd.prd_start_dt AS product_startdate
FROM silver.crm_prd_info prd
LEFT JOIN silver.erp_px_cat_g1v2 cat
ON prd.prd_cat = cat.id
WHERE PRD.prd_end_dt IS NULL

GO


IF OBJECT_ID ('gold.fact_sales', 'V') IS NOT NULL
	DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT 
	sls_ord_num AS order_number,
	cst.customer_key,
	pro.product_key,
	sls_order_dt AS order_date,
	sls_ship_dt AS shipping_date,
	sls_due_dt AS due_date,
	sls_price AS price,
	sls_quantity AS quantity,
	sls_sales AS sales_amount
FROM silver.crm_sales_details sal
LEFT JOIN gold.dim_customers cst
ON sal.sls_cust_id = cst.customer_id
LEFT JOIN gold.dim_products pro
ON sal.sls_prd_key = pro.product_number
