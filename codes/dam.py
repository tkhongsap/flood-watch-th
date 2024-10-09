# -*- coding: UTF-8 -*-

import requests
import datetime
import pandas as pd


def get_disaster_dam():
    url = "https://api-v3.thaiwater.net/api/v1/thaiwater30/analyst/dam"
    resp = requests.get(url)
    resp_json = resp.json()
    return resp_json['data']

def extractDamStation(data, dict_station, station_type):
    if data["dam"]["dam_name"]["th"] not in dict_station:
        # --- initial ---
        dict_station[data["dam"]["dam_name"]["th"]] = {}
        dict_station[data["dam"]["dam_name"]["th"]]["name"] = None
        dict_station[data["dam"]["dam_name"]["th"]]["lat"] = None
        dict_station[data["dam"]["dam_name"]["th"]]["lng"] = None
        dict_station[data["dam"]["dam_name"]["th"]]["oldcode"] = None
        dict_station[data["dam"]["dam_name"]["th"]]["min_storage"] = None
        dict_station[data["dam"]["dam_name"]["th"]]["max_storage"] = None
        dict_station[data["dam"]["dam_name"]["th"]]["normal_storage"] = None
        dict_station[data["dam"]["dam_name"]["th"]]["agency"] = None
        dict_station[data["dam"]["dam_name"]["th"]]["basin"] = None
        dict_station[data["dam"]["dam_name"]["th"]]["cctv"] = None
        dict_station[data["dam"]["dam_name"]["th"]]["station_type"] = None

    if "dam_name" in data["dam"] and "th" in data["dam"]["dam_name"]:
        dict_station[data["dam"]["dam_name"]["th"]]["name"] = data["dam"]["dam_name"]["th"]

    if "dam_lat" in data["dam"]:
        dict_station[data["dam"]["dam_name"]["th"]]["lat"] = data["dam"]["dam_lat"]

    if "dam_long" in data["dam"]:
        dict_station[data["dam"]["dam_name"]["th"]]["lng"] = data["dam"]["dam_long"]

    if "dam_oldcode" in data["dam"]:
        dict_station[data["dam"]["dam_name"]["th"]]["oldcode"] = data["dam"]["dam_oldcode"]

    if "min_storage" in data["dam"]:
        dict_station[data["dam"]["dam_name"]["th"]]["min_storage"] = data["dam"]["min_storage"]

    if "max_storage" in data["dam"]:
        dict_station[data["dam"]["dam_name"]["th"]]["max_storage"] = data["dam"]["max_storage"]

    if "normal_storage" in data["dam"]:
        dict_station[data["dam"]["dam_name"]["th"]]["normal_storage"] = data["dam"]["normal_storage"]

    if "agency" in data and "agency_name" in data["agency"] and "th" in data["agency"]["agency_name"]:
        dict_station[data["dam"]["dam_name"]["th"]]["agency"] = data["agency"]["agency_name"]["th"]

    if "basin" in data and "basin_name" in data["basin"] and "th" in data["basin"]["basin_name"]:
        dict_station[data["dam"]["dam_name"]["th"]]["basin"] = data["basin"]["basin_name"]["th"]

    if "cctv" in data and "url" in data["cctv"]:
        dict_station[data["dam"]["dam_name"]["th"]]["cctv"] = data["cctv"]["url"]

    dict_station[data["dam"]["dam_name"]["th"]]["station_type"] = station_type

    return dict_station.copy()

def extract_disaster_dam(dam_list):
    today = datetime.datetime.now()
    station_list = []
    data_list = []

    dict_station = {}
    # --- dam_hourly ---
    for data in dam_list["dam_hourly"]:
        # --- Station ---
        dict_station = extractDamStation(data, dict_station, 'อ่างขนาดใหญ่')

        # --- Data ---
        dict_data = {}
        dict_data["name"] = data["dam"]["dam_name"]["th"]
        dict_data["datetime"] = data["dam_date"]
        dict_data["storage"] = data["dam_storage"]
        dict_data["storage_percent"] = data["dam_storage_percent"]
        dict_data["inflow"] = data["dam_inflow"]
        dict_data["inflow_acc_percent"] = data["dam_inflow_acc_percent"]
        dict_data["uses_water"] = data["dam_uses_water"]
        dict_data["uses_water_percent"] = data["dam_uses_water_percent"]
        dict_data["level"] = data["dam_level"]
        dict_data["released"] = data["dam_released"]
        dict_data["spilled"] = data["dam_spilled"]
        dict_data["losses"] = data["dam_losses"]
        dict_data["evap"] = data["dam_evap"]
        dict_data["type"] = data["station_type"]
        dict_data["collected_at"] = today
        data_list.append(dict_data)

    # --- dam_daily ---
    for data in dam_list["dam_daily"]:
        # --- Station ---
        dict_station = extractDamStation(data, dict_station, 'อ่างขนาดใหญ่')

        # --- Data ---
        dict_data = {}
        dict_data["name"] = data["dam"]["dam_name"]["th"]
        dict_data["datetime"] = data["dam_date"]
        dict_data["storage"] = data["dam_storage"]
        dict_data["storage_percent"] = data["dam_storage_percent"]
        dict_data["inflow"] = data["dam_inflow"]
        dict_data["inflow_acc_percent"] = data["dam_inflow_acc_percent"]
        dict_data["inflow_avg"] = data["dam_inflow_avg"]
        dict_data["inflow_acc"] = data["dam_inflow_acc"]
        dict_data["uses_water"] = data["dam_uses_water"]
        dict_data["uses_water_percent"] = data["dam_uses_water_percent"]
        dict_data["uses_water_percent_calc"] = data["dam_uses_water_percent_calc"]
        dict_data["level"] = data["dam_level"]
        dict_data["released"] = data["dam_released"]
        dict_data["released_acc"] = data["dam_released_acc"]
        dict_data["spilled"] = data["dam_spilled"]
        dict_data["losses"] = data["dam_losses"]
        dict_data["evap"] = data["dam_evap"]
        dict_data["type"] = data["station_type"]
        dict_data["collected_at"] = today
        data_list.append(dict_data)

    # --- dam_medium ---
    for data in dam_list["dam_medium"]:
        # --- Station ---
        dict_station = extractDamStation(data, dict_station, 'อ่างขนาดกลาง')

        # --- Data ---
        dict_data = {}
        dict_data["name"] = data["dam"]["dam_name"]["th"]
        dict_data["datetime"] = data["dam_date"]
        dict_data["storage"] = data["dam_storage"]
        dict_data["storage_percent"] = data["dam_storage_percent"]
        dict_data["inflow"] = data["dam_inflow"]
        dict_data["uses_water"] = data["dam_uses_water"]
        dict_data["released"] = data["dam_released"]
        dict_data["type"] = "dam_medium"
        dict_data["collected_at"] = today
        data_list.append(dict_data)

    station_list = [dict_station[name].copy() for name in dict_station]

    return station_list, data_list

def dataframe_to_excel(df, file_name):
    return df.to_excel(file_name, index=False)

def main():
    dam_list = get_disaster_dam()
    dam_station, dam_data = extract_disaster_dam(dam_list)
    df_dam_station = pd.DataFrame(dam_station)
    df_dam_data = pd.DataFrame(dam_data)
    dataframe_to_excel(df_dam_station, './output/dam_station.xlsx')
    dataframe_to_excel(df_dam_data, './output/dam_data.xlsx')

if __name__ == "__main__":   
    main()