from datetime import datetime

import pyarrow as pa
import pyiceberg
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, StringType, NestedField
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
from pyiceberg.table.sorting import SortOrder, SortField


# Connect to the catalog.
print("Connect to the catalog")
catalog = load_catalog(
    "rest",
    **{
        "uri": "http://localhost:8182/",  # REST server URL.
        "type": "rest",
        "s3.endpoint": f"http://localhost:9002",  # Minio URI and credentials
        "s3.access-key-id": "minio",
        "s3.secret-access-key": "minio123",
    },
)

# Set up a namespace if it does not exist.
print("Create namespace iceberg")
try:
    catalog.create_namespace("iceberg")
    print("--Created")
except pyiceberg.exceptions.NamespaceAlreadyExistsError:
    print("--Already exists")

# List namespaces.
print("List namespaces")
ns_list = catalog.list_namespaces()
for ns in ns_list:
    print(ns)

# List tables and delete the names table if it exists.
print("List tables")
tab_list = catalog.list_tables("iceberg")
for tab in tab_list:
    print(tab, type(tab))
    if tab[0] == "iceberg" and tab[1] == "names":
        print("Dropping names table if it exists")
        catalog.drop_table("iceberg.names")

# Now create the test table. It's partitioned by name, and sorted by age.
schema = Schema(
    NestedField(field_id=1, name="name", field_type=StringType(), required=False),
    NestedField(field_id=2, name="age", field_type=DoubleType(), required=False),
)
partition_spec = PartitionSpec(
    PartitionField(
        source_id=1, field_id=1000, transform=IdentityTransform(), name="name"
    )
)
sort_order = SortOrder(SortField(source_id=2, transform=IdentityTransform()))
table = catalog.create_table(
    identifier="iceberg.names",
    schema=schema,
    location="s3://warehouse/data",
    partition_spec=partition_spec,
    sort_order=sort_order,
)


# Generate some trading data.
print("Add four rows of data to iceberg.names")
df = pa.Table.from_pylist(
    [
        {"name": "Alice", "age": 17.0},
        {"name": "Alice", "age": 19.0},
        {"name": "Bob","age": 20.0},
        {"name": "David","age": 22.0},
    ],
)
table.append(df)

# Read table with pyiceberg.
df = table.scan().to_pandas()
print(f"Table read with pyiceberg: \n{df} \n")

input("Script paused. Press Enter to continue...")

# Drop the table.
print("Drop table iceberg.names")
catalog.drop_table("iceberg.names")

# Recreate the table.
print("Recreate the table iceberg.names")
table = catalog.create_table(
    identifier="iceberg.names",
    schema=schema,
    location="s3://warehouse/data",
    partition_spec=partition_spec,
    sort_order=sort_order,
)

# Read table with pyiceberg.
df = table.scan().to_pandas()
print(f"Table read with pyiceberg: \n{df} \n")

input("Script paused. Press Enter to continue...")

# ____________________________________________

# Generate new trading data.
print("Add one row of data to iceberg.names")
df = pa.Table.from_pylist(
    [
        {"name": "Simon", "age": 30.0},
    ],
)
table.append(df)

# Read table with pyiceberg.
df = table.scan().to_pandas()
print(f"Table read with pyiceberg: \n{df} \n")

input("Script paused. Press Enter to continue...")