import duckdb
import pyarrow.dataset as ds
import timeit

delmt = '\t'

def check_csv():
    in_path = 'C:/Users/onkar.muttemwar/Projects/duckdb-explore/transactions.csv'
    out_path = './parquet_files/'

    data = ds.dataset(in_path, format = 'csv')
    print('csv',data.count_rows())

def check_parquet():
    out_path = './parquet_files/'
    data = ds.dataset(out_path, format = 'parquet')

    print('parquet',data.count_rows())

def check_duckdb():
    db = duckdb.connect()
    print(db.execute("SELECT COUNT(*) FROM parquet_scan('parquet_files/*.parquet')").fetchall())
    #print(db.execute("select * from duckdbperf").fetchall())

#csv_time: float = timeit.timeit(stmt = 'check_csv()',globals = globals(),number = 2)
parquet_time = timeit.timeit(stmt = 'check_parquet()',globals = globals(), number = 2)
duckdb_time = timeit.timeit(stmt = 'check_duckdb()',globals = globals(), number = 2)

#print(csv_time)
print(parquet_time)
print(duckdb_time)

#ds.write_dataset(data, out_path, format = "parquet", max_rows_per_file = 1e7)

