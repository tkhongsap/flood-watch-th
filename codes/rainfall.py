# -*- coding: UTF-8 -*-

import requests
import datetime
import pandas as pd



def process_station_data(station_type, station_data, dict_station, dict_data, today):
    tmp_id = None
    if station_type in ["rainfall_24h", "rainfall_daily", "rainfall_3days", "rainfall_7days", "rainfall_monthly",
                        "rainfall_yearly"]:
        tmp_id = station_data["station"]["id"]
    elif station_type in ["rainfall_yesterday"]:
        tmp_id = station_data["tele_station_id"]

    if tmp_id is None:
        return

    ##### Station #####
    if tmp_id not in dict_station:
        dict_station[tmp_id] = {}
        dict_station[tmp_id]["id"] = tmp_id
        dict_station[tmp_id]["name"] = None
        dict_station[tmp_id]["lat"] = None
        dict_station[tmp_id]["lng"] = None
        dict_station[tmp_id]["station_oldcode"] = None
        dict_station[tmp_id]["basin_code"] = None
        dict_station[tmp_id]["sub_basin_code"] = None
        dict_station[tmp_id]["basin_name"] = None
        dict_station[tmp_id]["agency_name"] = None
        dict_station[tmp_id]["collected_at"] = today

    if "station" in station_data and "tele_station_name" in station_data["station"] and "th" in station_data["station"][
        "tele_station_name"]:
        dict_station[tmp_id]["name"] = station_data["station"]["tele_station_name"]["th"]
    if "tele_station_name" in station_data and "th" in station_data["tele_station_name"]:
        dict_station[tmp_id]["name"] = station_data["tele_station_name"]["th"]

    if "station" in station_data and "tele_station_lat" in station_data["station"]:
        dict_station[tmp_id]["lat"] = station_data["station"]["tele_station_lat"]
    if "tele_station_lat" in station_data:
        dict_station[tmp_id]["lat"] = station_data["tele_station_lat"]

    if "station" in station_data and "tele_station_long" in station_data["station"]:
        dict_station[tmp_id]["lng"] = station_data["station"]["tele_station_long"]
    if "tele_station_long" in station_data:
        dict_station[tmp_id]["lng"] = station_data["tele_station_long"]

    if "station" in station_data and "tele_station_oldcode" in station_data["station"]:
        dict_station[tmp_id]["station_oldcode"] = station_data["station"]["tele_station_oldcode"]

    if "station" in station_data and "sub_basin_id" in station_data["station"]:
        dict_station[tmp_id]["sub_basin_code"] = station_data["station"]["sub_basin_id"]
    if "sub_basin_id" in station_data:
        dict_station[tmp_id]["sub_basin_code"] = station_data["sub_basin_id"]

    if "basin" in station_data and station_data["basin"] != None and "basin_code" in station_data["basin"]:
        dict_station[tmp_id]["basin_code"] = station_data["basin"]["basin_code"] if len(
            str(station_data["basin"]["basin_code"])) == 2 else "0%s" % (station_data["basin"]["basin_code"])
    if "station" in station_data and "basin_id" in station_data["station"]:
        dict_station[tmp_id]["basin_code"] = station_data["station"]["basin_id"] if len(
            str(station_data["station"]["basin_id"])) == 2 else "0%s" % (station_data["station"]["basin_id"])

    if "basin" in station_data and station_data["basin"] != None and "basin_name" in station_data["basin"] and "th" in \
            station_data["basin"]["basin_name"]:
        dict_station[tmp_id]["basin_name"] = station_data["basin"]["basin_name"]["th"]

    if "agency" in station_data and "agency_name" in station_data["agency"] and "th" in station_data["agency"][
        "agency_name"]:
        dict_station[tmp_id]["agency_name"] = station_data["agency"]["agency_name"]["th"]
    if "agency_name" in station_data and "th" in station_data["agency_name"]:
        dict_station[tmp_id]["agency_name"] = station_data["agency_name"]["th"]

    ##### Station Data #####
    if tmp_id not in dict_data:
        dict_data[tmp_id] = {}
        dict_data[tmp_id]["id"] = tmp_id
        dict_data[tmp_id]["rain_24h_value"] = None
        dict_data[tmp_id]["rain_24h_datetime"] = None
        dict_data[tmp_id]["rain_daily_value"] = None
        dict_data[tmp_id]["rain_daily_datetime"] = None
        dict_data[tmp_id]["rain_yesterday_value"] = None
        dict_data[tmp_id]["rain_yesterday_datetime"] = None
        dict_data[tmp_id]["rain_3days_value"] = None
        dict_data[tmp_id]["rain_3days_startdate"] = None
        dict_data[tmp_id]["rain_3days_enddate"] = None
        dict_data[tmp_id]["rain_7days_value"] = None
        dict_data[tmp_id]["rain_7days_startdate"] = None
        dict_data[tmp_id]["rain_7days_enddate"] = None
        dict_data[tmp_id]["rain_monthly_value"] = None
        dict_data[tmp_id]["rain_monthly_datetime"] = None
        dict_data[tmp_id]["rain_yearly_value"] = None
        dict_data[tmp_id]["rain_yearly_datetime"] = None

    if station_type == "rainfall_24h":
        if "rain_24h" in station_data and "rainfall_datetime" in station_data:
            dict_data[tmp_id]["rain_24h_value"] = station_data["rain_24h"]
            dict_data[tmp_id]["rain_24h_datetime"] = station_data["rainfall_datetime"]

    if station_type == "rainfall_daily":
        if "rainfall_value" in station_data and "rainfall_datetime" in station_data:
            dict_data[tmp_id]["rain_daily_value"] = station_data["rainfall_value"]
            dict_data[tmp_id]["rain_daily_datetime"] = station_data["rainfall_datetime"]

    if station_type == "rainfall_yesterday":
        if "rainfall_value" in station_data and "rainfall_datetime" in station_data:
            dict_data[tmp_id]["rain_yesterday_value"] = station_data["rainfall_value"]
            dict_data[tmp_id]["rain_yesterday_datetime"] = station_data["rainfall_datetime"]

    if station_type == "rainfall_3days":
        if "rain_3d" in station_data and "rainfall_start_date" in station_data and "rainfall_end_date" in station_data:
            dict_data[tmp_id]["rain_3days_value"] = station_data["rain_3d"]
            dict_data[tmp_id]["rain_3days_startdate"] = station_data["rainfall_start_date"]
            dict_data[tmp_id]["rain_3days_enddate"] = station_data["rainfall_end_date"]

    if station_type == "rainfall_7days":
        if "rain_7d" in station_data and "rainfall_start_date" in station_data and "rainfall_end_date" in station_data:
            dict_data[tmp_id]["rain_7days_value"] = station_data["rain_7d"]
            dict_data[tmp_id]["rain_7days_startdate"] = station_data["rainfall_start_date"]
            dict_data[tmp_id]["rain_7days_enddate"] = station_data["rainfall_end_date"]

    if station_type == "rainfall_monthly":
        if "rainfall_value" in station_data and "rainfall_datetime" in station_data:
            dict_data[tmp_id]["rain_monthly_value"] = station_data["rainfall_value"]
            dict_data[tmp_id]["rain_monthly_datetime"] = station_data["rainfall_datetime"]

    if station_type == "rainfall_yearly":
        if "rainfall_value" in station_data and "rainfall_datetime" in station_data:
            dict_data[tmp_id]["rain_yearly_value"] = station_data["rainfall_value"]
            dict_data[tmp_id]["rain_yearly_datetime"] = station_data["rainfall_datetime"]

def generate_disaster_rain_flow():
    rain_api_dict = {
        'rainfall_24h': "https://api-v3.thaiwater.net/api/v1/thaiwater30/public/rain_24h",
        'rainfall_daily': "https://api-v3.thaiwater.net/api/v1/thaiwater30/public/rain_today",
        'rainfall_yesterday': "https://api-v3.thaiwater.net/api/v1/thaiwater30/public/rain_yesterday",
        'rainfall_3days': "https://api-v3.thaiwater.net/api/v1/thaiwater30/provinces/rain3d",
        'rainfall_7days': "https://api-v3.thaiwater.net/api/v1/thaiwater30/provinces/rain7d",
        'rainfall_monthly': "https://api-v3.thaiwater.net/api/v1/thaiwater30/public/rain_monthly",
        'rainfall_yearly': "https://api-v3.thaiwater.net/api/v1/thaiwater30/public/rain_yearly",
    }

    for key, url in rain_api_dict.items():
        yield {
            'type': key,
            'url': url,
        }

def get_disaster_rain_data(rain_api):
    station_type = rain_api['type']
    url = rain_api['url']
    result_list = []

    for i in range(5):  # Try up to 5 times
        try:
            resp = requests.get(url)
            resp_obj = resp.json()
            result_list = resp_obj['data']
            break
        except:
            time.sleep(5)

    return {
        station_type: result_list
    }

def extract_disaster_rain_data(rain_data_list):
    today = datetime.datetime.now()
    dict_station = {}
    dict_station_data = {}
    for rain_data in rain_data_list:
        for station_type, station_data_list in rain_data.items():
            for station_data in station_data_list:
                process_station_data(station_type, station_data, dict_station, dict_station_data, today)
    return [value for value in dict_station.values()], [value for value in dict_station_data.values()]

def dataframe_to_excel(df, file_name):
    return df.to_excel(file_name, index=False)

def main():
    rain_data_list = []
    for rain_api in generate_disaster_rain_flow():
        rain_data_list.append(get_disaster_rain_data(rain_api))
    rain_station, rain_data = extract_disaster_rain_data(rain_data_list)
    df_rain_station = pd.DataFrame(rain_station)
    df_rain_data = pd.DataFrame(rain_data)
    dataframe_to_excel(df_rain_station, './output/rain_station.xlsx')
    dataframe_to_excel(df_rain_data, './output/rain_data.xlsx')

if __name__ == "__main__":   
    main()
