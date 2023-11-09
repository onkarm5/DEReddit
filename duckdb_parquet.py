import duckdb
db = duckdb.connect()
db.execute("CREATE VIEW duckdbperf AS SELECT COUNT(*) FROM parquet_scan('parquet_files/*.parquet')")
print(db.execute("select * from duckdbperf").fetchall())