import requests
import datetime
import pandas as pd
import time
import os
import logging
from typing import Dict, List, Tuple
import geopandas as gpd
from shapely.geometry import Point
import random
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(
    filename='data_processing.log',
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s:%(message)s'
)

def make_api_request(url: str, max_retries: int = 5, initial_delay: int = 5) -> Dict:
    """
    Makes an API request with retry logic and exponential backoff.

    :param url: API endpoint URL
    :param max_retries: Maximum number of retry attempts
    :param initial_delay: Initial delay between retries in seconds
    :return: JSON response as a dictionary
    """
    session = requests.Session()
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            logging.info(f"Successful API request to {url}")
            return response.json()
        except requests.RequestException as e:
            logging.error(f"Attempt {attempt}: Error making API request to {url}: {e}")
            if attempt < max_retries:
                sleep_time = initial_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
                logging.info(f"Retrying in {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
            else:
                logging.critical(f"Failed to get data from {url} after {max_retries} attempts")
                raise Exception(f"Failed to get data from {url} after {max_retries} attempts")

def save_to_excel_and_markdown(data: pd.DataFrame, base_filename: str) -> None:
    """
    Saves DataFrame to Excel and Markdown files.

    :param data: DataFrame to save
    :param base_filename: Base filename without extension
    """
    try:
        os.makedirs('output', exist_ok=True)
        excel_path = f'./output/{base_filename}.xlsx'
        markdown_path = f'./output/{base_filename}.md'
        
        # Save to Excel
        data.to_excel(excel_path, index=False)
        logging.info(f"Data saved to {excel_path}")
        
        # Save to Markdown
        with open(markdown_path, 'w', encoding='utf-8') as f:
            f.write(f"# {base_filename.replace('_', ' ').title()}\n\n")
            f.write(f"Generated on: {datetime.datetime.now()}\n\n")
            f.write(f"Total records: {len(data)}\n\n")
            f.write(data.to_markdown(index=False))
        logging.info(f"Data saved to {markdown_path}")
    except Exception as e:
        logging.error(f"Error saving data for {base_filename}: {e}")

def process_water_level() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Processes water level data from the API.

    :return: Tuple of station DataFrame and data DataFrame
    """
    url = "https://api-v3.thaiwater.net/api/v1/thaiwater30/public/waterlevel_load"
    response = make_api_request(url)
    data = response.get('waterlevel_data', {}).get('data', [])
    
    station_list = []
    data_list = []
    today = datetime.datetime.now()
    
    for item in data:
        station = item.get('station', {})
        if all(k in station for k in ["tele_station_lat", "tele_station_long"]) and "th" in station.get("tele_station_name", {}):
            station_list.append({
                "id": station.get('id'),
                "name": station['tele_station_name'].get('th'),
                "lat": station.get('tele_station_lat'),
                "lng": station.get('tele_station_long'),
                "station_oldcode": station.get('tele_station_oldcode'),
                "left_bank": station.get('left_bank'),
                "right_bank": station.get('right_bank'),
                "min_bank": station.get('min_bank'),
                "ground_level": station.get('ground_level'),
                "offset_": station.get('offset'),
                "basin_name": item.get('basin', {}).get('basin_name', {}).get('th'),
                "agency_name": item.get('agency', {}).get('agency_name', {}).get('th'),
                "is_key_station": station.get('is_key_station'),
                "warning_level_m": station.get('warning_level_m'),
                "critical_level_m": station.get('critical_level_m'),
                "critical_level_msl": station.get('critical_level_msl'),
                "collected_at": today
            })
        
        data_list.append({
            "id": station.get('id'),
            "datetime": item.get('waterlevel_datetime'),
            "waterlevel_m": item.get('waterlevel_m'),
            "waterlevel_msl": item.get('waterlevel_msl'),
            "waterlevel_msl_previous": item.get('waterlevel_msl_previous'),
            "flow_rate": item.get('flow_rate'),
            "discharge": item.get('discharge'),
            "storage_percent": item.get('storage_percent'),
            "situation_level": item.get('situation_level'),
            "collected_at": today
        })
    
    water_level_station = pd.DataFrame(station_list)
    water_level_data = pd.DataFrame(data_list)
    
    logging.info(f"Processed water level data: {water_level_station.shape[0]} stations, {water_level_data.shape[0]} data records.")
    
    return water_level_station, water_level_data

def process_water_gate() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Processes water gate data from the API.

    :return: Tuple of station DataFrame and data DataFrame
    """
    url = "https://api-v3.thaiwater.net/api/v1/thaiwater30/public/watergate_load"
    response = make_api_request(url)
    data = response.get('watergate_data', {}).get('data', [])
    
    station_list = []
    data_list = []
    today = datetime.datetime.now()
    
    for item in data:
        station = item.get('station', {})
        if all(k in station for k in ["tele_station_lat", "tele_station_long"]):
            station_list.append({
                "id": station.get('id'),
                "name": station['tele_station_name'].get('th'),
                "lat": station.get('tele_station_lat'),
                "lng": station.get('tele_station_long'),
                "station_oldcode": station.get('tele_station_oldcode'),
                "left_bank": station.get('left_bank'),
                "right_bank": station.get('right_bank'),
                "is_key_station": station.get('is_key_station'),
                "warning_level_m": station.get('warning_level_m'),
                "critical_level_m": station.get('critical_level_m'),
                "critical_level_msl": station.get('critical_level_msl'),
                "basin_name": item.get('basin', {}).get('basin_name', {}).get('th'),
                "agency_name": item.get('agency', {}).get('agency_name', {}).get('th'),
                "collected_at": today
            })
            
            data_list.append({
                "id": station.get('id'),
                "watergate_in": item.get('watergate_in'),
                "watergate_out": item.get('watergate_out'),
                "watergate_datetime_in": item.get('watergate_datetime_in'),
                "watergate_datetime_out": item.get('watergate_datetime_out'),
                "pump_on": item.get('pump_on'),
                "pump": item.get('pump'),
                "floodgate_open": item.get('floodgate_open'),
                "floodgate": item.get('floodgate'),
                "floodgate_height": item.get('floodgate_height'),
                "collected_at": today
            })
    
    water_gate_station = pd.DataFrame(station_list)
    water_gate_data = pd.DataFrame(data_list)
    
    logging.info(f"Processed water gate data: {water_gate_station.shape[0]} stations, {water_gate_data.shape[0]} data records.")
    
    return water_gate_station, water_gate_data

def process_rainfall() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Processes rainfall data from various API endpoints.

    :return: Tuple of station DataFrame and data DataFrame
    """
    rain_api_dict = {
        'rainfall_24h': "https://api-v3.thaiwater.net/api/v1/thaiwater30/public/rain_24h",
        'rainfall_daily': "https://api-v3.thaiwater.net/api/v1/thaiwater30/public/rain_today",
        'rainfall_yesterday': "https://api-v3.thaiwater.net/api/v1/thaiwater30/public/rain_yesterday",
        'rainfall_3days': "https://api-v3.thaiwater.net/api/v1/thaiwater30/provinces/rain3d",
        'rainfall_7days': "https://api-v3.thaiwater.net/api/v1/thaiwater30/provinces/rain7d",
        'rainfall_monthly': "https://api-v3.thaiwater.net/api/v1/thaiwater30/public/rain_monthly",
        'rainfall_yearly': "https://api-v3.thaiwater.net/api/v1/thaiwater30/public/rain_yearly",
    }
    
    station_dict = {}
    data_dict = {}
    today = datetime.datetime.now()
    
    for rain_type, url in rain_api_dict.items():
        try:
            data = make_api_request(url).get('data', [])
        except Exception as e:
            logging.error(f"Skipping {rain_type} due to API error: {e}")
            continue
        
        for item in data:
            station_id = item.get('station', {}).get('id') or item.get('tele_station_id')
            
            if not station_id:
                logging.warning(f"Skipping item with missing station ID in {rain_type}")
                continue
            
            if station_id not in station_dict:
                station_dict[station_id] = {
                    "id": station_id,
                    "name": item.get('station', {}).get('tele_station_name', {}).get('th') or item.get('tele_station_name', {}).get('th'),
                    "lat": item.get('station', {}).get('tele_station_lat') or item.get('tele_station_lat'),
                    "lng": item.get('station', {}).get('tele_station_long') or item.get('tele_station_long'),
                    "station_oldcode": item.get('station', {}).get('tele_station_oldcode'),
                    "basin_code": item.get('basin', {}).get('basin_code') or item.get('station', {}).get('basin_id'),
                    "sub_basin_code": item.get('station', {}).get('sub_basin_id') or item.get('sub_basin_id'),
                    "basin_name": item.get('basin', {}).get('basin_name', {}).get('th'),
                    "agency_name": item.get('agency', {}).get('agency_name', {}).get('th') or item.get('agency_name', {}).get('th'),
                    "collected_at": today
                }
                
                # Format basin_code to be two digits
                if station_dict[station_id].get("basin_code"):
                    basin_code = str(station_dict[station_id]["basin_code"])
                    station_dict[station_id]["basin_code"] = basin_code if len(basin_code) == 2 else f"0{basin_code}"
            
            if station_id not in data_dict:
                data_dict[station_id] = {
                    "id": station_id,
                    "rain_24h_value": None,
                    "rain_24h_datetime": None,
                    "rain_daily_value": None,
                    "rain_daily_datetime": None,
                    "rain_yesterday_value": None,
                    "rain_yesterday_datetime": None,
                    "rain_3days_value": None,
                    "rain_3days_startdate": None,
                    "rain_3days_enddate": None,
                    "rain_7days_value": None,
                    "rain_7days_startdate": None,
                    "rain_7days_enddate": None,
                    "rain_monthly_value": None,
                    "rain_monthly_datetime": None,
                    "rain_yearly_value": None,
                    "rain_yearly_datetime": None,
                    "collected_at": today
                }
            
            if rain_type == 'rainfall_24h':
                data_dict[station_id]["rain_24h_value"] = item.get('rain_24h')
                data_dict[station_id]["rain_24h_datetime"] = item.get('rainfall_datetime')
            elif rain_type == 'rainfall_daily':
                data_dict[station_id]["rain_daily_value"] = item.get('rainfall_value')
                data_dict[station_id]["rain_daily_datetime"] = item.get('rainfall_datetime')
            elif rain_type == 'rainfall_yesterday':
                data_dict[station_id]["rain_yesterday_value"] = item.get('rainfall_value')
                data_dict[station_id]["rain_yesterday_datetime"] = item.get('rainfall_datetime')
            elif rain_type == 'rainfall_3days':
                data_dict[station_id]["rain_3days_value"] = item.get('rain_3d')
                data_dict[station_id]["rain_3days_startdate"] = item.get('rainfall_start_date')
                data_dict[station_id]["rain_3days_enddate"] = item.get('rainfall_end_date')
            elif rain_type == 'rainfall_7days':
                data_dict[station_id]["rain_7days_value"] = item.get('rain_7d')
                data_dict[station_id]["rain_7days_startdate"] = item.get('rainfall_start_date')
                data_dict[station_id]["rain_7days_enddate"] = item.get('rainfall_end_date')
            elif rain_type == 'rainfall_monthly':
                data_dict[station_id]["rain_monthly_value"] = item.get('rainfall_value')
                data_dict[station_id]["rain_monthly_datetime"] = item.get('rainfall_datetime')
            elif rain_type == 'rainfall_yearly':
                data_dict[station_id]["rain_yearly_value"] = item.get('rainfall_value')
                data_dict[station_id]["rain_yearly_datetime"] = item.get('rainfall_datetime')
    
    rainfall_station = pd.DataFrame(list(station_dict.values()))
    rainfall_data = pd.DataFrame(list(data_dict.values()))
    
    logging.info(f"Processed rainfall data: {rainfall_station.shape[0]} stations, {rainfall_data.shape[0]} data records.")
    
    return rainfall_station, rainfall_data

def process_dam() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Processes dam data from the API.

    :return: Tuple of station DataFrame and data DataFrame
    """
    url = "https://api-v3.thaiwater.net/api/v1/thaiwater30/analyst/dam"
    response = make_api_request(url)
    dam_list = response.get('data', {})
    
    station_dict = {}
    data_list = []
    today = datetime.datetime.now()
    
    for dam_type in ['dam_hourly', 'dam_daily', 'dam_medium']:
        for item in dam_list.get(dam_type, []):
            dam = item.get('dam', {})
            dam_name = dam.get('dam_name', {}).get('th')
            if not dam_name:
                logging.warning(f"Dam entry missing 'dam_name' in {dam_type}")
                continue
            
            if dam_name not in station_dict:
                station_dict[dam_name] = {
                    "name": dam_name,
                    "lat": dam.get('dam_lat'),
                    "lng": dam.get('dam_long'),
                    "oldcode": dam.get('dam_oldcode'),
                    "min_storage": dam.get('min_storage'),
                    "max_storage": dam.get('max_storage'),
                    "normal_storage": dam.get('normal_storage'),
                    "agency": item.get('agency', {}).get('agency_name', {}).get('th'),
                    "basin": item.get('basin', {}).get('basin_name', {}).get('th'),
                    "cctv": item.get('cctv', {}).get('url'),
                    "station_type": 'อ่างขนาดใหญ่' if dam_type != 'dam_medium' else 'อ่างขนาดกลาง',
                    "collected_at": today
                }
            
            data_dict = {
                "name": dam_name,
                "datetime": item.get('dam_date'),
                "storage": item.get('dam_storage'),
                "storage_percent": item.get('dam_storage_percent'),
                "inflow": item.get('dam_inflow'),
                "uses_water": item.get('dam_uses_water'),
                "type": dam_type,
                "collected_at": today
            }
            
            if dam_type in ['dam_hourly', 'dam_daily']:
                data_dict.update({
                    "inflow_acc_percent": item.get('dam_inflow_acc_percent'),
                    "uses_water_percent": item.get('dam_uses_water_percent'),
                    "level": item.get('dam_level'),
                    "released": item.get('dam_released'),
                    "spilled": item.get('dam_spilled'),
                    "losses": item.get('dam_losses'),
                    "evap": item.get('dam_evap')
                })
                
                if dam_type == 'dam_daily':
                    data_dict.update({
                        "inflow_avg": item.get('dam_inflow_avg'),
                        "inflow_acc": item.get('dam_inflow_acc'),
                        "uses_water_percent_calc": item.get('dam_uses_water_percent_calc'),
                        "released_acc": item.get('dam_released_acc')
                    })
            
            data_list.append(data_dict)
    
    dam_station = pd.DataFrame(list(station_dict.values()))
    dam_data = pd.DataFrame(data_list)
    
    logging.info(f"Processed dam data: {dam_station.shape[0]} stations, {dam_data.shape[0]} data records.")
    
    return dam_station, dam_data

def validate_dataframe(df: pd.DataFrame, required_columns: List[str], dataset_name: str):
    """
    Validates the presence of required columns in a DataFrame.

    :param df: DataFrame to validate
    :param required_columns: List of required column names
    :param dataset_name: Name of the dataset for logging
    """
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        logging.warning(f"{dataset_name} is missing columns: {missing}")
    else:
        logging.info(f"All required columns present in {dataset_name}")

def validate_coordinates(df: pd.DataFrame, dataset_name: str):
    """
    Validates the latitude and longitude values in a DataFrame.

    :param df: DataFrame containing 'lat' and 'lng' columns
    :param dataset_name: Name of the dataset for logging
    """
    # Check for missing values
    missing = df[['lat', 'lng']].isnull().any().any()
    if missing:
        logging.warning(f"{dataset_name} contains missing latitude or longitude values.")
        # Optionally, save these records for further investigation
        invalid_coords = df[df[['lat', 'lng']].isnull().any(axis=1)]
        invalid_coords.to_excel('./output/invalid_coordinates.xlsx', index=False)
        logging.info("Invalid coordinate records saved to invalid_coordinates.xlsx")
    else:
        logging.info(f"All records in {dataset_name} have valid latitude and longitude.")
    
    # Check for out-of-bound values
    invalid_coords = df[
        (df['lat'] < -90) | (df['lat'] > 90) |
        (df['lng'] < -180) | (df['lng'] > 180)
    ]
    if not invalid_coords.empty:
        logging.warning(f"{dataset_name} contains out-of-bound latitude or longitude values.")
        # Optionally, save these records for further investigation
        invalid_coords.to_excel('./output/out_of_bound_coordinates.xlsx', index=False)
        logging.info("Out-of-bound coordinate records saved to out_of_bound_coordinates.xlsx")
    else:
        logging.info(f"All records in {dataset_name} have latitude between -90 and 90 and longitude between -180 and 180.")

def add_administrative_info(df: pd.DataFrame, gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Add province, amphur, and tambon information to the dataframe based on lat and lng.

    :param df: Input dataframe with 'lat' and 'lng' columns
    :param gdf: GeoDataFrame containing administrative boundaries
    :return: DataFrame with added administrative information
    """
    try:
        logging.info(f"Adding administrative information to DataFrame with shape {df.shape}")
        
        # Validate required columns
        validate_dataframe(df, ['lat', 'lng'], 'Input DataFrame')
        
        # Validate coordinates
        validate_coordinates(df, 'Input DataFrame')
        
        # Create a GeoDataFrame from the input DataFrame
        geometry = [Point(xy) for xy in zip(df['lng'], df['lat'])]
        gdf_points = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
        
        # Ensure shapefile is in the same CRS
        if gdf.crs != "EPSG:4326":
            logging.info(f"Shapefile CRS ({gdf.crs}) is not EPSG:4326. Converting CRS.")
            gdf = gdf.to_crs("EPSG:4326")
            logging.info("Shapefile CRS converted to EPSG:4326")
        else:
            logging.info("Shapefile CRS is already EPSG:4326")
        
        # Perform spatial join with 'intersects' predicate
        joined = gpd.sjoin(gdf_points, gdf, how="left", predicate="intersects")
        
        # Check for required columns in shapefile
        required_shapefile_columns = ['NAME_1', 'NAME_2', 'NAME_3']
        for col in required_shapefile_columns:
            if col not in joined.columns:
                logging.error(f"Shapefile is missing required column: {col}")
                raise ValueError(f"Shapefile is missing required column: {col}")
        
        # Add new columns to the original DataFrame
        df['province'] = joined['NAME_1']
        df['amphur'] = joined['NAME_2']
        df['tambon'] = joined['NAME_3']
        
        # Log the number of matched and unmatched points
        matched = df.dropna(subset=['province', 'amphur', 'tambon'])
        unmatched = df[df['province'].isna() | df['amphur'].isna() | df['tambon'].isna()]
        
        logging.info(f"Administrative information added successfully: {len(matched)} matched, {len(unmatched)} unmatched.")
        
        if not unmatched.empty:
            logging.warning(f"{len(unmatched)} records did not receive administrative information.")
            # Optionally, save unmatched records for further investigation
            unmatched.to_excel('./output/unmatched_records.xlsx', index=False)
            logging.info("Unmatched records saved to unmatched_records.xlsx")
        
        # Verify that the columns have been added
        if not all(col in df.columns for col in ['province', 'amphur', 'tambon']):
            logging.error("One or more location columns were not added to the DataFrame.")
            raise ValueError("One or more location columns were not added to the DataFrame.")
        
        return df
    except Exception as e:
        logging.error(f"Error adding administrative information: {e}")
        return df

def main():
    """
    Main function to orchestrate data processing and enrichment.
    """
    try:
        # Process Water Level
        water_level_station, water_level_data = process_water_level()
        save_to_excel_and_markdown(water_level_station, 'water_level_station')
        save_to_excel_and_markdown(water_level_data, 'water_level_data')
        time.sleep(random.uniform(5, 10))  # Add delay between dataset processing

        # Process Water Gate
        water_gate_station, water_gate_data = process_water_gate()
        save_to_excel_and_markdown(water_gate_station, 'water_gate_station')
        save_to_excel_and_markdown(water_gate_data, 'water_gate_data')
        time.sleep(random.uniform(5, 10))  # Add delay between dataset processing

        # Process Rainfall
        rainfall_station, rainfall_data = process_rainfall()
        save_to_excel_and_markdown(rainfall_station, 'rainfall_station')
        save_to_excel_and_markdown(rainfall_data, 'rainfall_data')
        time.sleep(random.uniform(5, 10))  # Add delay between dataset processing

        # Process Dam
        dam_station, dam_data = process_dam()
        save_to_excel_and_markdown(dam_station, 'dam_station')
        save_to_excel_and_markdown(dam_data, 'dam_data')

        # Perform joins to create final consolidated outputs
        combined_water_level = pd.merge(water_level_station, water_level_data, on='id', how='inner')
        combined_water_gate = pd.merge(water_gate_station, water_gate_data, on='id', how='inner')
        combined_rainfall = pd.merge(rainfall_station, rainfall_data, on='id', how='inner')
        combined_dam = pd.merge(dam_station, dam_data, on='name', how='inner')  # Ensure 'name' is unique

        # Save combined data without location information
        save_to_excel_and_markdown(combined_water_level, 'combined_water_level')
        save_to_excel_and_markdown(combined_water_gate, 'combined_water_gate')
        save_to_excel_and_markdown(combined_rainfall, 'combined_rainfall')
        save_to_excel_and_markdown(combined_dam, 'combined_dam')

        # Load the GADM Shapefile
        shapefile_path = "./shapefile/gadm41_THA_3.shp"  # Updated to .shp
        if not os.path.exists(shapefile_path):
            logging.warning(f"GADM Shapefile not found at {shapefile_path}")
            logging.warning("Administrative information will not be added to the datasets.")
            gdf = None
        else:
            try:
                gdf = gpd.read_file(shapefile_path)
                logging.info("GADM Shapefile loaded successfully")
                logging.info(f"GADM data contains {len(gdf)} rows")
                logging.info(f"GADM data columns: {gdf.columns.tolist()}")
                
                # Verify required columns
                required_columns = ['NAME_1', 'NAME_2', 'NAME_3']
                for col in required_columns:
                    if col not in gdf.columns:
                        logging.error(f"Shapefile is missing required column: {col}")
                        raise ValueError(f"Shapefile is missing required column: {col}")
            except Exception as e:
                logging.error(f"Error loading GADM Shapefile: {e}")
                gdf = None

        # Process datasets with or without administrative information
        datasets = [
            ("combined_water_level", combined_water_level),
            ("combined_water_gate", combined_water_gate),
            ("combined_rainfall", combined_rainfall),
            ("combined_dam", combined_dam)
        ]

        for name, df in datasets:
            logging.info(f"\nProcessing {name}...")
            logging.info(f"{name} shape: {df.shape}")
            logging.info(f"{name} columns: {df.columns.tolist()}")
            
            # Check for 'lat' and 'lng' columns
            if 'lat' not in df.columns or 'lng' not in df.columns:
                logging.warning(f"{name} is missing 'lat' or 'lng' columns. Skipping administrative information addition.")
                save_to_excel_and_markdown(df, f'{name}_without_location')
                continue
            
            if gdf is not None:
                try:
                    df_with_location = add_administrative_info(df, gdf)
                    logging.info(f"Successfully added location information to {name}")
                    save_to_excel_and_markdown(df_with_location, f'{name}_with_location')
                except Exception as e:
                    logging.error(f"Error processing {name}: {e}")
            else:
                logging.warning(f"Skipping administrative information for {name} due to missing GADM data")
                save_to_excel_and_markdown(df, f'{name}_without_location')
    
    except Exception as e:
        logging.critical(f"Critical error in main execution: {e}")

if __name__ == "__main__":
    main()