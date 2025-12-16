/*

create database and schem
=====================================
script purpose:
creating database named "DataWarehouse" and schemas after checking if it exists or no 


WARNNING:
this script will delete databse named 'DataWarehouse' if it exists

*/







USE master;

IF EXISTS(SELECT 1 FROM sys.databases WHERE name='DataWarehouse')

BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

CREATE DATABASE DataWarehouse;
GO

use DataWarehouse;
GO

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;


