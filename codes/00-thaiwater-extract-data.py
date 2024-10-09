import requests
import datetime
import pandas as pd
import time
import os
from typing import Dict, List, Tuple
import geopandas as gpd
from shapely.geometry import Point

def make_api_request(url: str, max_retries: int = 5, delay: int = 5) -> Dict:
    for _ in range(max_retries):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error making API request: {e}")
            time.sleep(delay)
    raise Exception(f"Failed to get data from {url} after {max_retries} attempts")

def save_to_excel_and_markdown(data: pd.DataFrame, base_filename: str) -> None:
    os.makedirs('output', exist_ok=True)
    excel_path = f'./output/{base_filename}.xlsx'
    markdown_path = f'./output/{base_filename}.md'
    
    # Save to Excel
    data.to_excel(excel_path, index=False)
    print(f"Data saved to {excel_path}")
    
    # Save to Markdown
    with open(markdown_path, 'w', encoding='utf-8') as f:
        f.write(f"# {base_filename.replace('_', ' ').title()}\n\n")
        f.write(f"Generated on: {datetime.datetime.now()}\n\n")
        f.write(f"Total records: {len(data)}\n\n")
        f.write(data.to_markdown(index=False))
    print(f"Data saved to {markdown_path}")

def process_water_level() -> Tuple[pd.DataFrame, pd.DataFrame]:
    url = "https://api-v3.thaiwater.net/api/v1/thaiwater30/public/waterlevel_load"
    data = make_api_request(url)['waterlevel_data']['data']
    
    station_list = []
    data_list = []
    today = datetime.datetime.now()
    
    for item in data:
        station = item['station']
        if "tele_station_lat" in station and "tele_station_long" in station and "th" in station["tele_station_name"]:
            station_list.append({
                "id": station['id'],
                "name": station['tele_station_name']['th'],
                "lat": station['tele_station_lat'],
                "lng": station['tele_station_long'],
                "station_oldcode": station['tele_station_oldcode'],
                "left_bank": station.get('left_bank'),
                "right_bank": station.get('right_bank'),
                "min_bank": station.get('min_bank'),
                "ground_level": station.get('ground_level'),
                "offset_": station.get('offset'),
                "basin_name": item['basin']['basin_name']['th'],
                "agency_name": item['agency']['agency_name']['th'],
                "is_key_station": station.get('is_key_station'),
                "warning_level_m": station.get('warning_level_m'),
                "critical_level_m": station.get('critical_level_m'),
                "critical_level_msl": station.get('critical_level_msl'),
                "collected_at": today
            })
        
        data_list.append({
            "id": station['id'],
            "datetime": item['waterlevel_datetime'],
            "waterlevel_m": item['waterlevel_m'],
            "waterlevel_msl": item['waterlevel_msl'],
            "waterlevel_msl_previous": item['waterlevel_msl_previous'],
            "flow_rate": item['flow_rate'],
            "discharge": item['discharge'],
            "storage_percent": item['storage_percent'],
            "situation_level": item.get('situation_level'),
            "collected_at": today
        })
    
    return pd.DataFrame(station_list), pd.DataFrame(data_list)

def process_water_gate() -> Tuple[pd.DataFrame, pd.DataFrame]:
    url = "https://api-v3.thaiwater.net/api/v1/thaiwater30/public/watergate_load"
    data = make_api_request(url)['watergate_data']['data']
    
    station_list = []
    data_list = []
    today = datetime.datetime.now()
    
    for item in data:
        station = item['station']
        if "tele_station_lat" in station and "tele_station_long" in station:
            station_list.append({
                "id": station['id'],
                "name": station['tele_station_name']['th'],
                "lat": station['tele_station_lat'],
                "lng": station['tele_station_long'],
                "station_oldcode": station['tele_station_oldcode'],
                "left_bank": station['left_bank'],
                "right_bank": station['right_bank'],
                "is_key_station": station['is_key_station'],
                "warning_level_m": station['warning_level_m'],
                "critical_level_m": station['critical_level_m'],
                "critical_level_msl": station['critical_level_msl'],
                "basin_name": item['basin']['basin_name']['th'],
                "agency_name": item['agency']['agency_name']['th'],
                "collected_at": today
            })
            
            data_list.append({
                "id": station['id'],
                "watergate_in": item['watergate_in'],
                "watergate_out": item['watergate_out'],
                "watergate_datetime_in": item['watergate_datetime_in'],
                "watergate_datetime_out": item['watergate_datetime_out'],
                "pump_on": item['pump_on'],
                "pump": item['pump'],
                "floodgate_open": item['floodgate_open'],
                "floodgate": item['floodgate'],
                "floodgate_height": item['floodgate_height'],
                "collected_at": today
            })
    
    return pd.DataFrame(station_list), pd.DataFrame(data_list)

def process_rainfall() -> Tuple[pd.DataFrame, pd.DataFrame]:
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
        data = make_api_request(url)['data']
        for item in data:
            station_id = item.get('station', {}).get('id') or item.get('tele_station_id')
            
            if not station_id:
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
                
                # Format basin_code
                if station_dict[station_id]["basin_code"]:
                    station_dict[station_id]["basin_code"] = station_dict[station_id]["basin_code"] if len(str(station_dict[station_id]["basin_code"])) == 2 else f"0{station_dict[station_id]['basin_code']}"
            
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
    
    return pd.DataFrame(list(station_dict.values())), pd.DataFrame(list(data_dict.values()))

def process_dam() -> Tuple[pd.DataFrame, pd.DataFrame]:
    url = "https://api-v3.thaiwater.net/api/v1/thaiwater30/analyst/dam"
    dam_list = make_api_request(url)['data']
    
    station_dict = {}
    data_list = []
    today = datetime.datetime.now()
    
    for dam_type in ['dam_hourly', 'dam_daily', 'dam_medium']:
        for item in dam_list[dam_type]:
            dam_name = item['dam']['dam_name']['th']
            if dam_name not in station_dict:
                station_dict[dam_name] = {
                    "name": dam_name,
                    "lat": item['dam'].get('dam_lat'),
                    "lng": item['dam'].get('dam_long'),
                    "oldcode": item['dam'].get('dam_oldcode'),
                    "min_storage": item['dam'].get('min_storage'),
                    "max_storage": item['dam'].get('max_storage'),
                    "normal_storage": item['dam'].get('normal_storage'),
                    "agency": item.get('agency', {}).get('agency_name', {}).get('th'),
                    "basin": item.get('basin', {}).get('basin_name', {}).get('th'),
                    "cctv": item.get('cctv', {}).get('url'),
                    "station_type": 'อ่างขนาดใหญ่' if dam_type != 'dam_medium' else 'อ่างขนาดกลาง',
                    "collected_at": today
                }
            
            data_dict = {
                "name": dam_name,
                "datetime": item['dam_date'],
                "storage": item['dam_storage'],
                "storage_percent": item['dam_storage_percent'],
                "inflow": item['dam_inflow'],
                "uses_water": item['dam_uses_water'],
                "type": dam_type,
                "collected_at": today
            }
            
            if dam_type in ['dam_hourly', 'dam_daily']:
                data_dict.update({
                    "inflow_acc_percent": item['dam_inflow_acc_percent'],
                    "uses_water_percent": item['dam_uses_water_percent'],
                    "level": item['dam_level'],
                    "released": item['dam_released'],
                    "spilled": item['dam_spilled'],
                    "losses": item['dam_losses'],
                    "evap": item['dam_evap']
                })
                
                if dam_type == 'dam_daily':
                    data_dict.update({
                        "inflow_avg": item['dam_inflow_avg'],
                        "inflow_acc": item['dam_inflow_acc'],
                        "uses_water_percent_calc": item['dam_uses_water_percent_calc'],
                        "released_acc": item['dam_released_acc']
                    })
            
            data_list.append(data_dict)
    
    return pd.DataFrame(list(station_dict.values())), pd.DataFrame(data_list)

def add_administrative_info(df: pd.DataFrame, gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Add province, amphur, and tambon information to the dataframe based on lat and lng.
    
    :param df: Input dataframe with 'lat' and 'lng' columns
    :param gdf: GeoDataFrame containing administrative boundaries
    :return: DataFrame with added administrative information
    """
    try:
        print(f"Input DataFrame shape: {df.shape}")
        print(f"Input DataFrame columns: {df.columns}")
        print(f"GeoDataFrame shape: {gdf.shape}")
        print(f"GeoDataFrame columns: {gdf.columns}")
        
        # Check if 'lat' and 'lng' columns exist
        if 'lat' not in df.columns or 'lng' not in df.columns:
            raise ValueError("Input DataFrame is missing 'lat' or 'lng' columns")
        
        # Create a GeoDataFrame from the input DataFrame
        geometry = [Point(xy) for xy in zip(df['lng'], df['lat'])]
        gdf_points = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
        
        # Reset index for both GeoDataFrames to avoid duplicates
        gdf_points = gdf_points.reset_index(drop=True)
        gdf = gdf.reset_index(drop=True)
        
        # Perform spatial join
        joined = gpd.sjoin(gdf_points, gdf, how="left", predicate="within")
        
        # Add new columns to the original DataFrame
        df['province'] = joined['NAME_1']
        df['amphur'] = joined['NAME_2']
        df['tambon'] = joined['NAME_3']
        
        print("Administrative information added successfully")
        return df
    except Exception as e:
        print(f"Error adding administrative information: {str(e)}")
        return df

def main():
    # Process Water Level
    water_level_station, water_level_data = process_water_level()
    save_to_excel_and_markdown(water_level_station, 'water_level_station')
    save_to_excel_and_markdown(water_level_data, 'water_level_data')

    # Process Water Gate
    water_gate_station, water_gate_data = process_water_gate()
    save_to_excel_and_markdown(water_gate_station, 'water_gate_station')
    save_to_excel_and_markdown(water_gate_data, 'water_gate_data')

    # Process Rainfall
    rainfall_station, rainfall_data = process_rainfall()
    save_to_excel_and_markdown(rainfall_station, 'rainfall_station')
    save_to_excel_and_markdown(rainfall_data, 'rainfall_data')

    # Process Dam
    dam_station, dam_data = process_dam()
    save_to_excel_and_markdown(dam_station, 'dam_station')
    save_to_excel_and_markdown(dam_data, 'dam_data')

    # Perform joins to create final consolidated outputs
    combined_water_level = pd.merge(water_level_station, water_level_data, on='id', how='inner')
    combined_water_gate = pd.merge(water_gate_station, water_gate_data, on='id', how='inner')
    combined_rainfall = pd.merge(rainfall_station, rainfall_data, on='id', how='inner')
    combined_dam = pd.merge(dam_station, dam_data, on='name', how='inner')

    # Save combined data without location information
    save_to_excel_and_markdown(combined_water_level, 'combined_water_level')
    save_to_excel_and_markdown(combined_water_gate, 'combined_water_gate')
    save_to_excel_and_markdown(combined_rainfall, 'combined_rainfall')
    save_to_excel_and_markdown(combined_dam, 'combined_dam')

    # Load the GADM GeoJSON file
    gadm_file_path = "./shapefile/gadm41_THA_3.json"
    if not os.path.exists(gadm_file_path):
        print(f"Warning: GADM GeoJSON file not found at {gadm_file_path}")
        print("Administrative information will not be added to the datasets.")
        gdf = None
    else:
        try:
            gdf = gpd.read_file(gadm_file_path)
            print("GADM GeoJSON file loaded successfully")
            print(f"GADM data contains {len(gdf)} rows")
            print(f"GADM data columns: {gdf.columns}")
        except Exception as e:
            print(f"Error loading GADM GeoJSON file: {e}")
            gdf = None

    # Process datasets with or without administrative information
    for name, df in [
        ("combined_water_level", combined_water_level),
        ("combined_water_gate", combined_water_gate),
        ("combined_rainfall", combined_rainfall),
        ("combined_dam", combined_dam)
    ]:
        print(f"\nProcessing {name}...")
        print(f"{name} shape: {df.shape}")
        print(f"{name} columns: {df.columns}")
        
        # Check for 'lat' and 'lng' columns
        if 'lat' not in df.columns or 'lng' not in df.columns:
            print(f"Warning: {name} is missing 'lat' or 'lng' columns")
            continue
        
        if gdf is not None:
            try:
                df_with_location = add_administrative_info(df, gdf)
                print(f"Successfully added location information to {name}")
                save_to_excel_and_markdown(df_with_location, f'{name}_with_location')
                print(f"Saved {name}_with_location")
            except Exception as e:
                print(f"Error processing {name}: {str(e)}")
        else:
            print(f"Skipping administrative information for {name} due to missing GADM data")
            save_to_excel_and_markdown(df, f'{name}_without_location')
            print(f"Saved {name}_without_location")

if __name__ == "__main__":
    main()