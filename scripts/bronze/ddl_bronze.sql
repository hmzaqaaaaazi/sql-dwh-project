CREATE OR REPLACE FUNCTION bronze.load_bronze()
RETURNS void AS
$$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration INTERVAL;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
BEGIN
    batch_start_time := NOW();

    start_time := NOW();

    RAISE NOTICE '===================';
    RAISE NOTICE 'Bronze data load';
    RAISE NOTICE '===================';
    TRUNCATE TABLE bronze.crm_cust_info;
    COPY bronze.crm_cust_info
    FROM '/mnt/c/Users/hmzaq/Desktop/Summers/POST_SQL/DWH/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
    WITH (
        FORMAT CSV,
        HEADER true,
        DELIMITER ','
    );
    end_time := NOW();
    duration := end_time - start_time;
    RAISE NOTICE '>> Duration: % seconds', EXTRACT(EPOCH FROM duration);


    start_time := NOW();

    RAISE NOTICE '===================';
    RAISE NOTICE 'Loading CRM tables';
    RAISE NOTICE '===================';
    TRUNCATE TABLE bronze.crm_prd_info;
    COPY bronze.crm_prd_info
    FROM '/mnt/c/Users/hmzaq/Desktop/Summers/POST_SQL/DWH/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
    WITH (
        FORMAT CSV,
        HEADER true,
        DELIMITER ','
    );

    end_time := NOW();
    duration := end_time - start_time;
    RAISE NOTICE '>> Duration: % seconds', EXTRACT(EPOCH FROM duration);



    start_time := NOW();

    TRUNCATE TABLE bronze.crm_sales_details;
    COPY bronze.crm_sales_details
    FROM '/mnt/c/Users/hmzaq/Desktop/Summers/POST_SQL/DWH/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
    WITH (
        FORMAT CSV,
        HEADER true,
        DELIMITER ','
    );

    end_time := NOW();
    duration := end_time - start_time;
    RAISE NOTICE '>> Duration: % seconds', EXTRACT(EPOCH FROM duration);


    start_time := NOW();

    RAISE NOTICE '===================';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '===================';
    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12 
    FROM '/mnt/c/Users/hmzaq/Desktop/Summers/POST_SQL/DWH/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
    WITH (
        FORMAT CSV,
        HEADER true,
        DELIMITER ','
    );

    end_time := NOW();
    duration := end_time - start_time;
    RAISE NOTICE '>> Duration: % seconds', EXTRACT(EPOCH FROM duration);



    start_time := NOW();

    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101
    FROM '/mnt/c/Users/hmzaq/Desktop/Summers/POST_SQL/DWH/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
    WITH (
        FORMAT CSV,
        HEADER true,
        DELIMITER ','
    );

    end_time := NOW();
    duration := end_time - start_time;
    RAISE NOTICE '>> Duration: % seconds', EXTRACT(EPOCH FROM duration);

    start_time := NOW();

    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2
    FROM '/mnt/c/Users/hmzaq/Desktop/Summers/POST_SQL/DWH/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
    WITH (
        FORMAT CSV,
        HEADER true,
        DELIMITER ','
    );

    end_time := NOW();
    duration := end_time - start_time;
    RAISE NOTICE '>> Duration: % seconds', EXTRACT(EPOCH FROM duration);

    batch_end_time := NOW();
    RAISE NOTICE 'Batch duration: % seconds', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));


    EXCEPTION
    WHEN others THEN
        RAISE NOTICE '================================================';
        RAISE NOTICE 'ERROR LOADING BRONZE DATA: %', SQLERRM;
        RAISE NOTICE 'SQL STATE: %', SQLSTATE;
        RAISE NOTICE '================================================';
        -- Re-raise the exception to stop execution
        RAISE;
END;
$$
LANGUAGE plpgsql;
