-- table crm_cust_info
-- Check for Nulls and duplicates in Primary keys
-- Expectations zero

SELECT cst_id, COUNT(*) FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- cleaning the nulls from primary key cst_id from bronze.crm_cust_info
SELECT
*
FROM ( SELECT
*,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flagfirst
FROM bronze.crm_cust_info) WHERE flagfirst = 1;


-- Check for unwanted spaces (first_name)
SELECT cst_firstname FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- Check for unwanted spaces (last_name)
SELECT cst_lastname FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- Check for unwanted spaces (gndr)
SELECT cst_gndr FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

-- Check for unwanted spaces (marital_status)
SELECT cst_marital_status FROM bronze.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status);

-- Data standardization and consistency
SELECT DISTINCT(cst_gndr) FROM bronze.crm_cust_info;
SELECT DISTINCT(cst_marital_status) FROM bronze.crm_cust_info;

----------

-- Checking Qulaity of Silver

-- Check for Nulls and duplicates in Primary keys
-- Expectations zero

SELECT cst_id, COUNT(*) FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1;

-- Check for unwanted spaces (first_name)
SELECT cst_firstname FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- Check for unwanted spaces (last_name)
SELECT cst_lastname FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- Check for unwanted spaces (gndr)
SELECT cst_gndr FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr);

-- Check for unwanted spaces (marital_status)
SELECT cst_marital_status FROM silver.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status);

-- Data standardization and consistency
SELECT DISTINCT(cst_gndr) FROM silver.crm_cust_info;
SELECT DISTINCT(cst_marital_status) FROM silver.crm_cust_info;

------------------------------------------------

--crm_prd_info
-- check the nulls and dupplicates in the primary key

SELECT prd_id, COUNT(*) FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;
--Result everything is fine 

-- Check for unwanted spaces
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);
-- Result nothing to change

-- Check for Nulls and Negative numbers 
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;
-- RESULT onlt nulls values found change them to 0

-- DATA standardization and Consistency

SELECT DISTINCT(prd_line)
FROM bronze.crm_prd_info;

-- CHECK for invalid date orders
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


-- Data Qulaity checks for Silver

SELECT prd_id, COUNT(*) FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;
--Result everything is fine 

-- Check for unwanted spaces
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);
-- Result nothing to change

-- Check for Nulls and Negative numbers 
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;
-- RESULT onlt nulls values found change them to 0

-- DATA standardization and Consistency

SELECT DISTINCT(prd_line)
FROM silver.crm_prd_info;

-- CHECK for invalid date orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-------------------------------------------

--crm_sales_details

-- Check for unwanted spaces
SELECT sls_ord_num
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);
-- Result nothing to change

-- also see whether all the prd_key and cust_id in sales table match the primary keys of other tables

SELECT sls_prd_key FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);

SELECT sls_cust_id FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT crm_cust_info.cst_id FROM silver.crm_cust_info);
-- results everything matches between these tables


-- CHECK for invalid dates (sls_ord_dt)

SELECT NULLIF(sls_order_dt,0) AS sls_order_dt FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LENGTH(sls_order_dt::TEXT) != 8
OR sls_order_dt > 20500101 OR sls_order_dt < 19000101;

-- CHECK for invalid dates (sls_ship_dt)

SELECT NULLIF(sls_ship_dt,0) AS sls_ship_dt FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 OR LENGTH(sls_ship_dt::TEXT) != 8
OR sls_ship_dt > 20500101 OR sls_ship_dt < 19000101;

-- CHECK for invalid dates (sls_due_dt)

SELECT NULLIF(sls_due_dt,0) AS sls_due_dt FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 OR LENGTH(sls_due_dt::TEXT) != 8
OR sls_due_dt > 20500101 OR sls_due_dt < 19000101;


-- CHECK for invalid date orders
SELECT *
FROM bronze.crm_sales_details
WHERE sls_ship_dt < sls_order_dt OR sls_order_dt > sls_due_dt;

-- BUSINESS RULE Sales = quantity * price &
-- negative, zeros and nulls are not allowed
SELECT
sls_sales,
sls_quantity,
sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0;

-- handling these zeros, nulls and negatives
SELECT
sls_sales AS old_sales,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * sls_price
        THEN sls_quantity * sls_price
    ELSE sls_sales
END AS sls_sales, 
sls_quantity,
sls_price AS old_price,
CASE WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity,0)
    ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0;

-------------------------------------------------------------
-- erp_cust_az12 Table

-- also see whether all the cid and cust_id in sales table match the primary keys of other tables
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
    ELSE cid
END AS cid,
bdate,
gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
    ELSE cid
END NOT IN (SELECT DISTINCT(cst_key) FROM silver.crm_cust_info);

-- Identify out of range dates
SELECT DISTINCT(bdate) FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > NOW();
-- result alot of unacceptable dates shows bad data needs to be handled
-- in this case we will only transform the dates that are 100 percent incorrect for example the future dates and turn them into NULLs

-- data consistency & standardization

SELECT DISTINCT(gen) FROM bronze.erp_cust_az12;
-- results alot of incconsistency needs to be handled

-- Quality check for the silver table

-- Identify out of range dates
SELECT DISTINCT(bdate) FROM silver.erp_cust_az12
WHERE bdate > NOW();

-- data consistency & standardization

SELECT DISTINCT(gen) FROM silver.erp_cust_az12;

-----------------------------------------------------
-- erp_loc_a101 (table)

-- check whether the cid and cst_key from crm_cust_info have same values or not
-- there is a problem with the cid column as there is '-' in the id number due to which these are not matching so we need to remove them
SELECT
REPLACE(cid, '-', '') AS cid
FROM bronze.erp_loc_a101
WHERE cid NOT IN (SELECT cst_key FROM silver.crm_cust_info) ;

-- DATA STANDARDIZATION AND CONSISTENCY
SELECT DISTINCT(cntry)
FROM bronze.erp_loc_a101
ORDER BY cntry;

-- silver quality checks already done above with the insert but can also be done seperately

-- erp_px_cat_g1v2

-- CHECK FOR UNWANTED SPACES
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

--- result all good

-- DATA standardization and consistency

SELECT DISTINCT(cat) FROM bronze.erp_px_cat_g1v2;
SELECT DISTINCT(subcat) FROM bronze.erp_px_cat_g1v2;
SELECT DISTINCT(maintenance) FROM bronze.erp_px_cat_g1v2;
--- EVERYTHING good here too

-- no quality checks for silver as data from bronze layer came in perfect condition
