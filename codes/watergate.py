# -*- coding: UTF-8 -*-

import requests
import datetime
import pandas as pd

def get_disaster_watergate():
    url = "https://api-v3.thaiwater.net/api/v1/thaiwater30/public/watergate_load"
    resp = requests.get(url)
    resp_json = resp.json()
    return resp_json['watergate_data']['data']

def extract_disaster_watergate(watergate_list):
    today = datetime.datetime.now()
    station_list = []
    data_list = []
    for data in watergate_list:
        if "tele_station_lat" in data["station"] and "tele_station_long" in data["station"]:
            #--- Station ---
            dict_station = {}
            dict_station["id"] = data["station"]["id"]
            dict_station["name"] = data["station"]["tele_station_name"]["th"]
            dict_station["lat"] = data["station"]["tele_station_lat"]
            dict_station["lng"] = data["station"]["tele_station_long"]
            dict_station["station_oldcode"] = data["station"]["tele_station_oldcode"]
            dict_station["left_bank"] = data["station"]["left_bank"]
            dict_station["right_bank"] = data["station"]["right_bank"]
            dict_station["is_key_station"] = data["station"]["is_key_station"]
            dict_station["warning_level_m"] = data["station"]["warning_level_m"]
            dict_station["critical_level_m"] = data["station"]["critical_level_m"]
            dict_station["critical_level_msl"] = data["station"]["critical_level_msl"]
            dict_station["basin_name"] = data["basin"]["basin_name"]["th"]
            dict_station["agency_name"] = data["agency"]["agency_name"]["th"]
            dict_station["collected_at"] = today
            station_list.append(dict_station)

            #--- Data ---
            dict_data = {}
            dict_data["id"] = data["station"]["id"]
            dict_data["watergate_in"] = data["watergate_in"]
            dict_data["watergate_out"] = data["watergate_out"]
            dict_data["watergate_datetime_in"] = data["watergate_datetime_in"]
            dict_data["watergate_datetime_out"] = data["watergate_datetime_out"]
            dict_data["pump_on"] = data["pump_on"]
            dict_data["pump"] = data["pump"]
            dict_data["floodgate_open"] = data["floodgate_open"]
            dict_data["floodgate"] = data["floodgate"]
            dict_data["floodgate_height"] = data["floodgate_height"]
            dict_data["collected_at"] = today
            data_list.append(dict_data)

    return station_list, data_list

def dataframe_to_excel(df,file_name):
    return df.to_excel(file_name,index=False)

def main():
    watergate_list = get_disaster_watergate()
    station_list, data_list = extract_disaster_watergate(watergate_list)
    df_water_gate_station = pd.DataFrame(station_list)
    df_water_gate_data = pd.DataFrame(station_list)
    dataframe_to_excel(df_water_gate_station,'./output/water_gate_station.xlsx')
    dataframe_to_excel(df_water_gate_data,'./output/water_gate_data.xlsx')

if __name__ == "__main__":   
    main()