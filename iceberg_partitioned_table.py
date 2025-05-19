import pyiceberg
import pyarrow as pa

from pyiceberg.catalog import load_rest
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, StringType, NestedField, LongType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform, BucketTransform
from pyiceberg.table.sorting import SortOrder


print("Connect to the catalog")
catalog = load_rest(
    name="rest",
    conf={
        "uri": "http://localhost:8182/",
        "type": "rest",
        "s3.endpoint": f"http://localhost:9002",
        "s3.access-key-id": "minio",
        "s3.secret-access-key": "minio123",
    },
)


print("Create namespace iceberg")
try:
    catalog.create_namespace("iceberg")
    print("--Created")
except pyiceberg.exceptions.NamespaceAlreadyExistsError:
    print("--Already exists")

print("List namespaces")
ns_list = catalog.list_namespaces()
for ns in ns_list:
    print(ns)

print("List tables")
tab_list = catalog.list_tables("iceberg")
for tab in tab_list:
    print(tab, type(tab))
    if tab[0] == "iceberg" and tab[1] == "names":
        print("Dropping names table if it exists")
        catalog.drop_table("iceberg.names")

schema = Schema(
    NestedField(field_id=1, name="name", field_type=StringType(), required=False),
    NestedField(field_id=2, name="double", field_type=DoubleType(), required=False),
    NestedField(field_id=3, name="integer", field_type=LongType(), required=False),
)
partition_spec = PartitionSpec(
    PartitionField(
        source_id=1,
        field_id=1001,
        transform=BucketTransform(num_buckets=4),
        name="symbol_partition",
    ),
    PartitionField(
        source_id=2,
        field_id=1002,
        transform=IdentityTransform(),
        name="double_partition",
    ),
    PartitionField(
        source_id=3,
        field_id=1003,
        transform=IdentityTransform(),
        name="integer_partition",
    ),
)

table = catalog.create_table(
    identifier="iceberg.names",
    schema=schema,
    location="s3://warehouse/data",
    partition_spec=partition_spec,
    sort_order=SortOrder(),
)


length = 10
integer_values = [i for i in range(length)]
double_values = [i + 0.5 for i in integer_values]
name_values = [f"name_{i}" for i in integer_values]
data = [
    {
        "name": name_values[i],
        "double": double_values[i],
        "integer": integer_values[i],
    }
    for i in range(length)
]
df = pa.Table.from_pylist(data)
table.append(df)


df = table.scan().to_pandas()
print(f"Table read with pyiceberg: \n{df} \n")


input("Press Enter to continue...")
