import duckdb
import pyarrow.dataset as ds

out_path = './parquet_files/'
data = ds.dataset(out_path, format = 'parquet')

print(data.count_rows())
