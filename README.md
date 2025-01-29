# iceberg-clickhouse-integration

Reproduction was created on base of the https://github.com/Altinity/clickhouse-sql-examples/tree/main/iceberg/iceberg-with-minio


## Bring up containers
```
docker compose up -d
```

## Run the script
It creates iceberg table, adds 4 rows into it, drop the table, recreates it, adds one row to recreated table.
```
python3 iceberg_recreate_table.py
```

## Connect to clickhouse client in container

```
docker exec -it clickhouse bash
clickhouse client
```

## Issue with recreate table
When I drop the table and recreate it (with same name), I still see old data when reading it from clickhouse.

Reproduction:
1. Bring up containers
2. Run the script 
```
Connect to the catalog
Create namespace iceberg
--Already exists
List namespaces
('iceberg',)
List tables
('iceberg', 'names') <class 'tuple'>
Dropping names table if it exists
Add four rows of data to iceberg.names
Table read with pyiceberg: 
    name   age
0  Alice  17.0
1  Alice  19.0
2    Bob  20.0
3  David  22.0
```
3. Connect to clickhouse client (in other terminal window) in container and read the same table using Iceberg engine
```
DROP DATABASE IF EXISTS datalake;
SET allow_experimental_database_iceberg=true;
CREATE DATABASE datalake
ENGINE = Iceberg('http://rest:8181/v1', 'minio', 'minio123')
SETTINGS catalog_type = 'rest', storage_endpoint = 'http://minio:9000/warehouse', warehouse = 'iceberg' ;
SELECT * FROM datalake.`iceberg.names`;
```
Output:
```
   ┌─name──┬─age─┐
1. │ David │  22 │
   └───────┴─────┘
   ┌─name──┬─age─┐
2. │ Alice │  17 │
3. │ Alice │  19 │
   └───────┴─────┘
   ┌─name─┬─age─┐
4. │ Bob  │  20 │
   └──────┴─────┘

4 rows in set. Elapsed: 0.026 sec. 
```
4. Return to script window and press enter  
Output:
```
Drop table iceberg.names
Recreate the table iceberg.names
Table read with pyiceberg: 
Empty DataFrame
Columns: [name, age]
Index: []
```
5. Again connect to clickhouse client (in other terminal window) in container and read the same table using Iceberg engine
```
DROP DATABASE IF EXISTS datalake;
SET allow_experimental_database_iceberg=true;
CREATE DATABASE datalake
ENGINE = Iceberg('http://rest:8181/v1', 'minio', 'minio123')
SETTINGS catalog_type = 'rest', storage_endpoint = 'http://minio:9000/warehouse', warehouse = 'iceberg' ;
SELECT * FROM datalake.`iceberg.names`;
```
Output:
```
   ┌─name─┬─age─┐
1. │ Bob  │  20 │
   └──────┴─────┘
   ┌─name──┬─age─┐
2. │ Alice │  17 │
3. │ Alice │  19 │
   └───────┴─────┘
   ┌─name──┬─age─┐
4. │ David │  22 │
   └───────┴─────┘

4 rows in set. Elapsed: 0.019 sec. 
```
Here I expect to see empty table.  

6. Return to script window and press enter  
Output:
```
Script paused. Press Enter to continue...
Add one row of data to iceberg.names
Table read with pyiceberg: 
    name   age
0  Simon  30.0 
```
7. Again connect to clickhouse client (in other terminal window) in container and read the same table using Iceberg engine
```
DROP DATABASE IF EXISTS datalake;
SET allow_experimental_database_iceberg=true;
CREATE DATABASE datalake
ENGINE = Iceberg('http://rest:8181/v1', 'minio', 'minio123')
SETTINGS catalog_type = 'rest', storage_endpoint = 'http://minio:9000/warehouse', warehouse = 'iceberg' ;
SELECT * FROM datalake.`iceberg.names`;
```
Output:
```
   ┌─name──┬─age─┐
1. │ David │  22 │
   └───────┴─────┘
   ┌─name──┬─age─┐
2. │ Alice │  17 │
3. │ Alice │  19 │
   └───────┴─────┘
   ┌─name─┬─age─┐
4. │ Bob  │  20 │
   └──────┴─────┘

4 rows in set. Elapsed: 0.020 sec. 
```
But I expect to see one row as I see when reading with pyiceberg.


I expecrince the same issue when reading data with iceberg table function as well.
I use following query:
```
SELECT * FROM iceberg('http://minio:9000/warehouse/data')
```

