/*
==================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
==================================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to
    populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
    - Truncates Silver tables.
    - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver
==================================================================================
*/

CREATE OR ALTER PROCEDURE  silver.load_silver AS
BEGIN
    print '>>truncating silver.crm_cust_info'
    TRUNCATE TABLE silver.crm_cust_info
    print '>> insering into silver.crm_cust_info'
    INSERT INTO silver.crm_cust_info(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date)

    select
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' then 'Single'
         when UPPER(TRIM(cst_marital_status))='M' then 'Married'
                  ELSE 'n/a'
    END marital_status,
    CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
         when UPPER(TRIM(cst_gndr))='M' then 'Male'
                  ELSE 'n/a'
    END cst_gndr,
    cst_create_date
    FROM(
    select *,
    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
    from 
    bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
    )t where flag_last=1


    print '>>truncating silver.crm_prd_info'
    TRUNCATE TABLE silver.crm_prd_info
    print '>> insering into silver.crm_prd_info'
    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT 
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') AS cat_id,  --extract category id
    SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key, --extract product key
    prd_nm,
    ISNULL(prd_cost,0) AS prd_cost,
    CASE UPPER(TRIM(prd_line))
         WHEN 'M' THEN 'Mountain'
         WHEN 'R' THEN 'Road'
         WHEN 'S' THEN 'other Sales'
         WHEN 'T' THEN 'Touring'
         ELSE 'n/a'
    END AS prd_line,  --map product line cde to values
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt  --calculate dateas one date earlier from nect start date
    from bronze.crm_prd_info


    print '>>truncating silver.crm_sales_details'
    TRUNCATE TABLE silver.crm_sales_details
    print '>> insering into silver.crm_sales_details'
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
    CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt)! = 8 THEN null
          ELSE cast(cast(sls_order_dt as VARCHAR)AS DATE) 
    END AS sls_order_dt,
    CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt)! = 8 THEN null
          ELSE cast(cast(sls_ship_dt as VARCHAR)AS DATE) 
    END AS sls_ship_dt,
    CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt)! = 8 THEN null
          ELSE cast(cast(sls_due_dt as VARCHAR)AS DATE) 
    END AS sls_due_dt,
    CASE WHEN sls_sales is null or sls_sales != sls_quantity * ABS(sls_price)
         then sls_quantity *ABS(sls_price)
         else sls_sales
    END as sls_sales     ,
    sls_quantity,
    case when sls_price is null or sls_price <=0 
    then sls_sales/nullif(sls_quantity,0)
    else sls_price
    end as sls_price
    FROM bronze.crm_sales_details


    print '>>truncating silver.erp_cust_az12'
    TRUNCATE TABLE silver.erp_cust_az12
    print '>> insering into silver.erp_cust_az12'
    INSERT INTO silver.erp_cust_az12 (cid,bdate,gen)
    SELECT 
    CASE WHEN cid like 'NAS%' then substring(cid,4,len(cid))
         else cid
    end as cid,
    CASE WHEN bdate > GETDATE() THEN NULL
         ELSE bdate
    end as bdate,
    CASE WHEN UPPER(TRIM(gen)) in ('F','FEMALE') THEN 'Female'
         WHEN UPPER(TRIM(gen)) in ('M','MALE') THEN 'Male'
         else 'n/a'
    end as gen

    FROM bronze.erp_cust_az12




    print '>>truncating silver.erp_loc_a101'
    TRUNCATE TABLE silver.erp_loc_a101
    print '>> insering into silver.erp_loc_a101'
    INSERT INTO silver.erp_loc_a101(cid,cntry) 
    select
    case when cid like 'AW-%' THEN REPLACE(cid,'-','')
    else cid
    end as cid,
    CASE 
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry
    from bronze.erp_loc_a101;




    print '>>truncating silver.erp_px_cat_g1v2'
    TRUNCATE TABLE silver.erp_px_cat_g1v2
    print '>> insering into silver.erp_px_cat_g1v2'
    insert into silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
    SELECT 
    id,
    cat,
    subcat,
    maintenance
    from bronze.erp_px_cat_g1v2
END
