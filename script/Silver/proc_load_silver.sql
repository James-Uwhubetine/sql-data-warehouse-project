/*
===================================================================================
Stored Procedure: Load the Silver Layer (Bronze -> Silver)
===================================================================================
Script Purpose: 
	This Stored Procedure perform the ETL (Extract, Transform, Load) process to populate 
	the 'silver' schema tables from the 'bronze' schema.

Action Performed: 
	It performs the following functions:
	- Truncate the tables in the bronze schema before loading data
	- Uses the 'INSERT' command to load clean and transformed data from 'bronze' tables
	  to the 'silver tables'

Parameter: None
	This Stored Procedure does not accept parameters or return any values

Usage Example:
	EXEC silver.load_bronze
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE()
		PRINT '===============================================================';
		PRINT 'Loading the Silver Layer';
		PRINT '===============================================================';

		PRINT '---------------------------------------------------------------';
		PRINT 'Loading the CRM Tables';
		PRINT '---------------------------------------------------------------';

		SET @start_time = GETDATE()
		PRINT '<< Truncating Table: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info
		PRINT '>> Inserting Table: silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,cst_create_date
		)
			SELECT
				cst_id,
				cst_key,
				TRIM(cst_firstname) cst_firstname,
				TRIM(cst_lastname) cst_lastname,
				CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
					 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
					 ELSE 'Unavalable'
					 END cst_marital_status,
				--cst_gndr,
				CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
					 WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
					 ELSE 'Unavalable'
					 END cst_gndr,
				cst_create_date
			FROM (
				SELECT 
					*,
					ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) flag
				FROM bronze.crm_cust_info
				WHERE cst_id IS NOT NULL
				)t
			WHERE flag = 1
			SET @end_time = GETDATE();
			PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
			PRINT '------------------------------------------------------------------------'

		SET @start_time = GETDATE()
		PRINT '<< Truncating Table: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info
		PRINT '>> Inserting Table: silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info (
			prd_id,
			prd_cat,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
			SELECT 
				prd_id,
				REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') prd_cat,
				SUBSTRING(prd_key, 7, LEN(prd_key)) prd_key,
				prd_nm,
				COALESCE(prd_cost, 0) prd_cost,
				CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
					 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
					 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
					 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
					 ELSE 'Unavalable'
					 END prd_line,
				CAST(prd_start_dt AS DATE) prd_start_date,
				CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) prd_end_dt
			FROM bronze.crm_prd_info
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '------------------------------------------------------------------------'

		SET @start_time = GETDATE()
		PRINT '<< Truncating Table: silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details
		PRINT '>> Inserting Table: silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
				END sls_order_dt,
			CASE WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
				END sls_ship_dt,
			CASE WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
				END sls_due_dt,
			CASE WHEN sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) OR sls_sales IS NULL 
				THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
				END sls_sales,
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <= 0
				THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price
				END sls_price
		FROM bronze.crm_sales_details
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '------------------------------------------------------------------------'
		
		PRINT '---------------------------------------------------------------';
		PRINT 'Loading the ERP Tables';
		PRINT '---------------------------------------------------------------';
		
		SET @start_time = GETDATE()
		PRINT '<< Truncating Table: silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12
		PRINT '>> Inserting Table: silver.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
		SELECT 
			CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
				ELSE CID
				END cid,
			CASE WHEN BDATE > GETDATE() THEN NULL
				ELSE BDATE
				END bdate,
			CASE WHEN TRIM(GEN) IN ('M', 'MALE') THEN 'Male'
				WHEN TRIM(GEN) IN ('F', 'FEMALE') THEN 'Female'
				ELSE 'Unavailable'
				END gen
		FROM bronze.erp_cust_az12
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '------------------------------------------------------------------------'

		SET @start_time = GETDATE()
		PRINT '<< Truncating Table: silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101
		PRINT '>> Inserting Table: silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101 (
			cid,
			cntry
		)
		SELECT
			REPLACE(CID, '-', '') cid,
			CASE WHEN TRIM(CNTRY) IN ('USA', 'US') THEN 'United States'
				WHEN TRIM(CNTRY) IN ('DE') THEN 'Germany'
				WHEN TRIM(CNTRY) IS NULL OR TRIM(CNTRY) = '' THEN 'Unavalable'
				ELSE CNTRY
				END cntry
		FROM bronze.erp_loc_a101
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '------------------------------------------------------------------------'

		SET @start_time = GETDATE()
		PRINT '<< Truncating Table: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2
		PRINT '>> Inserting Table: silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '------------------------------------------------------------------------'

		SET @batch_end_time = GETDATE()

		PRINT '======================================================================='
		PRINT 'Loading Silver Layer is completed'
		PRINT '>> Total Load Duration ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds'
		PRINT '========================================================================'
	END TRY

	BEGIN CATCH
		PRINT '===========================================================================';
		PRINT 'ERROR OCCURED DURING LOADING THE SILVER LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Number' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '===========================================================================';
	END CATCH
END
