import os
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, DoubleType, StringType, LongType
import polars as pl

# setup environment
os.environ["PYICEBERG_HOME"] = os.getcwd()

# initialize th iceberg catalog
catalog = load_catalog(name="local")
print(catalog.properties)

# 3 creating an iceberg table
# 3.1 create Schema

# Create a schema that matches Polars defaults (i64, f64, nullable)
schema = Schema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=False),
    NestedField(field_id=2, name="category", field_type=StringType(), required=False),
    NestedField(field_id=3, name="amount", field_type=DoubleType(), required=False),
)
# 3.2 create NameSpace ie (database)
catalog.create_namespace_if_not_exists("transactions")
# 3.3 create the table
if catalog.table_exists("transactions.sales_data"):
    catalog.drop_table("transactions.sales_data")

iceberg_table = catalog.create_table(
    identifier="transactions.sales_data", schema=schema
)
#3.4 inspect the table
#print(iceberg_table.schema())

data = [
    {'id': 1, 'category': 'electronics', 'amount': 299.99},
    {'id': 2, 'category': 'clothing', 'amount': 79.99},
    {'id': 3, 'category': 'groceries', 'amount': 45.50},
    {'id': 4, 'category': 'electronics', 'amount': 999.99},
    {'id': 5, 'category': 'clothing', 'amount': 120.00},
]

df = pl.DataFrame(data)
df.write_iceberg(iceberg_table, mode="append")
df.write_iceberg(iceberg_table, mode="append")
