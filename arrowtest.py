# import pyarrow.parquet as pq

# table2 = pq.read_table(
#     "/home/mattnhb/Clonadinhos/feature_store/AGGREGATED/part-00000-313fc117-7497-470c-9ac2-083efe37eb7a-c000.snappy.parquet"
# )

# result = table2.to_batches(25)
# print(f"{len(result)=}")
# print(table2.to_pandas())

import pandas as pd
from pprint import pprint
dfs = pd.read_parquet(
    "/home/mattnhb/Clonadinhos/feature_store/AGGREGATED/part-00000-313fc117-7497-470c-9ac2-083efe37eb7a-c000.snappy.parquet",
    engine="pyarrow",
    chunked= 25
)
print(f"{dfs=}")
# results = df.to_dict(orient="records") 
# for result in results:
#     pprint(result)
#     exit()