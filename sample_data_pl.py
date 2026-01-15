import polars as pl

import os
from pyiceberg.catalog import load_catalog
from pprint import pprint

# must match how you wrote it
os.environ["PYICEBERG_HOME"] = "/home/bobo/Desktop/iceberg_demos/iceberg_catalog/transactions/sales_data"

catalog = load_catalog("local")

# load the table
tbl = catalog.load_table("transactions.sales_data")

df = pl.scan_iceberg(tbl)

print(df.collect())

snaps = tbl.snapshots()
tbl.append()
for snap in snaps:
    pprint(snap.model_dump())
tbl.scan()

