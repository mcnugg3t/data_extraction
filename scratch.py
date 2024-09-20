
## import
import rasterio
import re
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

from local_functions import id_unique_dates, extract_multiple_tifs, extract_tif_to_coords

import pyarrow
import fastparquet

## test extraction function
print(f"import pyarrow version {pyarrow.__version__}")

# test the performance of extract_tif_to_coords using subsample_pts
subsample_pts_pth = 'F:/PFET/mid/eco_extr_points.parquet'
subsample_pts = pd.read_parquet(subsample_pts_pth)
print(subsample_pts.head())

tst_tif_pth = "F:/PFET/ECOSTRESS/emissivity/lste001/2021_1/ECO2LSTE.001_SDS_Emis2_doy2021001091738_aid0001.tif"
tst_extract = extract_tif_to_coords(tst_tif_pth, subsample_pts, return_df=True, var_name='Emis2', DBG=True, DBG_SUBS=True)

tst_extract['geometry'] = tst_extract.apply(lambda row: Point(row['longitude'], row['latitude']), axis=1)

tst_extract = tst_extract[tst_extract['Emis2'].notnull() & tst_extract['Emis2'] != 0].copy().reset_index(drop=True)

tst_extract = tst_extract.sample(frac=0.1, random_state=42).copy().reset_index()
tst_extract['Emis2'] = 0.49 + tst_extract['Emis2'] * 0.002

print(f"len(tst_extract) after subsampling: {len(tst_extract)}")

gdf = gpd.GeoDataFrame(tst_extract, geometry='geometry')
gdf.set_crs(epsg=4326, inplace=True)
gdf.to_file("F:/PFET/tst_extract/tst_extract.shp", driver='ESRI Shapefile')