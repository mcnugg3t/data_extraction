## import
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


## first merge the two cloud datasets and filter to subset where no cloud mask
cloud001_pth = 'F:/PFET/results/cloud/cloud001.parquet'
cloud002_pth = 'F:/PFET/results/cloud/cloud002.parquet'

## second, load ecostress data by chunks, filtering to subset of IDX and datetime where no cloud is present