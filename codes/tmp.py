import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import os

# Create a DataFrame from the coordinates
data = {
    'lat': [13.69498, 14.14605, 16.96218, 14.75102, 19.351, 15.63915, 16.00651, 14.92595, 15.34439],
    'lng': [100.63465, 100.62136, 104.52514, 100.41464, 99.82497, 102.039345, 100.122246, 100.275826, 100.04724]
}

df = pd.DataFrame(data)

def add_administrative_info(df, gdf):
    try:
        geometry = [Point(xy) for xy in zip(df['lng'], df['lat'])]
        gdf_points = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
        
        if gdf.crs != "EPSG:4326":
            gdf = gdf.to_crs("EPSG:4326")
        
        joined = gpd.sjoin(gdf_points, gdf, how="left", predicate="intersects")
        
        # Add English names
        df['province_en'] = joined['NAME_1']
        df['amphur_en'] = joined['NAME_2']
        df['tambon_en'] = joined['NAME_3']
        
        # Add Thai names
        df['province_th'] = joined['NL_NAME_1']
        df['amphur_th'] = joined['NL_NAME_2']
        df['tambon_th'] = joined['NL_NAME_3']
        
        return df
    except Exception as e:
        print(f"Error in add_administrative_info: {e}")
        raise

# Load the GADM Shapefile
shapefile_path = "./shapefile/gadm41_THA_3.shp"

# Set the configuration option to restore or create .shx file if needed
os.environ['SHAPE_RESTORE_SHX'] = 'YES'

try:
    gdf = gpd.read_file(shapefile_path)
except Exception as e:
    print(f"Error loading shapefile: {e}")
    print("Please check if all shapefile components are present in the specified directory.")
    exit(1)

try:
    # Add administrative information to our DataFrame
    df_with_location = add_administrative_info(df, gdf)

    # Display the results
    columns_to_display = ['lat', 'lng', 'province_en', 'province_th', 'amphur_en', 'amphur_th', 'tambon_en', 'tambon_th']
    print(df_with_location[columns_to_display])

    # Optionally, save the results to a CSV file
    df_with_location.to_csv('output_with_thai_names.csv', index=False, encoding='utf-8-sig')
    print("Results saved to 'output_with_thai_names.csv'")
except Exception as e:
    print(f"An error occurred: {e}")