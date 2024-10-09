# -*- coding: UTF-8 -*-

import requests
import datetime
import pandas as pd

def get_disaster_waterlevel():
    url = "https://api-v3.thaiwater.net/api/v1/thaiwater30/public/waterlevel_load"
    resp = requests.get(url)
    resp_json = resp.json()
    return resp_json['waterlevel_data']['data']

def extract_disaster_waterlevel(waterlevel_list):
    today = datetime.datetime.now()
    station_list = []
    data_list = []
    for data in waterlevel_list:
        # --- Station ---
        if "tele_station_lat" in data["station"] and "tele_station_long" in data["station"] and "th" in data["station"][
            "tele_station_name"]:
            dict_station = {}
            dict_station["id"] = data["station"]["id"]
            dict_station["name"] = data["station"]["tele_station_name"]["th"]
            dict_station["lat"] = data["station"]["tele_station_lat"]
            dict_station["lng"] = data["station"]["tele_station_long"]
            dict_station["station_oldcode"] = data["station"]["tele_station_oldcode"]
            dict_station["left_bank"] = data["station"]["left_bank"] if "left_bank" in data["station"] else None
            dict_station["right_bank"] = data["station"]["right_bank"] if "right_bank" in data["station"] else None
            dict_station["min_bank"] = data["station"]["min_bank"] if "min_bank" in data["station"] else None
            dict_station["ground_level"] = data["station"]["ground_level"] if "ground_level" in data[
                "station"] else None
            dict_station["offset_"] = data["station"]["offset"] if "offset" in data["station"] else None
            dict_station["basin_name"] = data["basin"]["basin_name"]["th"]
            dict_station["agency_name"] = data["agency"]["agency_name"]["th"]
            dict_station["is_key_station"] = data["station"]["is_key_station"] if "is_key_station" in data[
                "station"] else None
            dict_station["warning_level_m"] = data["station"]["warning_level_m"] if "warning_level_m" in data[
                "station"] else None
            dict_station["critical_level_m"] = data["station"]["critical_level_m"] if "critical_level_m" in data[
                "station"] else None
            dict_station["critical_level_msl"] = data["station"]["critical_level_msl"] if "critical_level_msl" in data[
                "station"] else None
            dict_station["collected_at"] = today
            station_list.append(dict_station)

        # --- Data ---
        dict_data = {}
        dict_data["id"] = data["station"]["id"]
        dict_data["datetime"] = data["waterlevel_datetime"]
        dict_data["waterlevel_m"] = data["waterlevel_m"]
        dict_data["waterlevel_msl"] = data["waterlevel_msl"]
        dict_data["waterlevel_msl_previous"] = data["waterlevel_msl_previous"]
        dict_data["flow_rate"] = data["flow_rate"]
        dict_data["discharge"] = data["discharge"]
        dict_data["storage_percent"] = data["storage_percent"]
        dict_data["situation_level"] = data["situation_level"] if "situation_level" in data else None
        dict_data["collected_at"] = today
        data_list.append(dict_data)
    return station_list, data_list

def dataframe_to_excel(df, file_name):
    return df.to_excel(file_name, index=False)

def main():
    waterlevel_list = get_disaster_waterlevel()
    station_list, data_list = extract_disaster_waterlevel(waterlevel_list)
    df_waterlevel_station = pd.DataFrame(station_list)
    df_waterlevel_data = pd.DataFrame(station_list)
    dataframe_to_excel(df_waterlevel_station, './output/waterlevel_station.xlsx')
    dataframe_to_excel(df_waterlevel_data, './output/waterlevel_data.xlsx')

if __name__ == "__main__":   
    main()