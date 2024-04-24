import json
from datetime import datetime
from pymongo import MongoClient
import requests
import random
import time
from pymongo.errors import BulkWriteError

client = MongoClient('mongodb+srv://Developer:Bahubhashak@bahubhashaak-project.ascwu.mongodb.net/EMRI?retryWrites=true&w=majority')
print("Connected successfully!!!")
guardianAngel = client['GuardianAngel']
events = guardianAngel['events']

urls = {
    "http://onem2m.iiit.ac.in:443/~/in-cse/in-name/AE-WE/WE-VN04-00/Data/la",
    "http://onem2m.iiit.ac.in:443/~/in-cse/in-name/AE-AQ/AQ-KN00-00/Data/la",
    "http://onem2m.iiit.ac.in:443/~/in-cse/in-name/AE-AQ/AQ-PL00-00/Data/la",
    "http://onem2m.iiit.ac.in:443/~/in-cse/in-name/AE-AQ/AQ-MG00-00/Data/la"
}
# url = "http://onem2m.iiit.ac.in:443/~/in-cse/in-name/AE-WE/WE-VN04-00/Data/la"
payload = {}
headers = {
  'X-M2M-Origin': 'iiith_guest:iiith_guest',
  'Accept': 'application/json'
}

def generate_short_id():
    # Generate a short unique identifier based on current timestamp and random number
    timestamp = int(time.time() * 1000)  # Current timestamp in milliseconds
    rand_num = random.randint(0, 999999)  # Random number between 0 and 999999
    short_id = f"{timestamp}-{rand_num:06d}"  # Combine timestamp and random number
    return short_id
index = 0

def generate_description(data):
    description = "Environmental conditions recorded on " + data["Timestamp"] + ". "
    description += f"The temperature was moderately warm at {data['Temperature']}°C with a relative humidity of {data['Relative Humidity']}%, indicating a somewhat dry atmosphere. "
    description += f"Solar radiation was low at {data['Solar Radiation']} kW/m². "
    if data["Wind Speed"] == 0.0:
        description += "There was no wind at the time of recording. "
    else:
        description += f"Wind was blowing from {data['Wind Direction']} degrees at {data['Wind Speed']} m/s. "
    if data["Rain"] > 0.0:
        description += f"Rain was recorded at {data['Rain']} mm. "
    else:
        description += "No rainfall was recorded. "
    description += f"Barometric pressure was relatively high at {data['Pressure']} hPa. "
    return description

def gen_description(data):
    description = (
        f"Data was recorded at timestamp {data['Timestamp']}. "
        f"The calibrated PM2.5 concentration is {data['Calibrated PM2.5']} , while the PM10 concentration is {data['PM10']}  "
        f"with a calibrated value of {data['Calibrated PM10']}. "
        f"Temperature was measured at {data['Temperature']}°C, adjusted to a calibrated temperature of {data['Calibrated Temperature']}°C. "
        f"Relative humidity stood at {data['Relative Humidity']}%, with a calibrated value of {data['Calibrated Relative Humidity']}%. "
        f"The AQI (Air Quality Index) recorded was {data['AQI']}, with an AQL (Air Quality Level) of {data['AQL']}. "
        f"The AQI-MP (AQI Measurement Precision) recorded as {data['AQI-MP']}. "
        f"The data interval for this measurement was {data['Data Interval']} minutes."
    )
    return description

def weather(response):
    parsed_data = json.loads(response)
    timestamp, solar_radiation, temperature, relative_humidity, wind_direction, wind_speed, gust_speed, dew_point, battery_voltage, rain, pressure = json.loads(parsed_data["m2m:cin"]["con"])

# Convert timestamp to datetime object
    timestamp = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    
# Create a dictionary with the parsed data
    parsed_json_data = {
        "Timestamp": timestamp,
        "Solar Radiation": solar_radiation,
        "Temperature": temperature,
        "Relative Humidity": relative_humidity,
        "Wind Direction": wind_direction,
        "Wind Speed": wind_speed,
        "Gust Speed": gust_speed,
        "Dew Point": dew_point,
        "Battery DC Voltage": battery_voltage,
        "Rain": rain,
        "Pressure": pressure
    }
    parsed_json_data['eventId'] = generate_short_id()
    parsed_json_data['latitude'] = '17.5497'
    parsed_json_data['longitude'] = '78.1230'
    parsed_json_data['headline'] = generate_description(parsed_json_data)
    parsed_json_data['visited'] = False
    parsed_json_data['dampingFactor'] = 1
    parsed_json_data['upvotes'] = 0
    parsed_json_data['downvotes'] = 0
    parsed_json_data['reports'] = 0
# Convert dictionary to JSON
    parsed_json = json.dumps(parsed_json_data, indent=4)

# Store in file
    with open('parsed_sensor_data0.json', 'w') as f:
        f.write(parsed_json)
    existing_doc = events.find_one({'headline': parsed_json_data['headline']})
    if existing_doc is None:
        # If the headline does not exist, insert the document
        events.insert_one(parsed_json_data)
    print("Data stored in 'parsed_sensor_data.json' file.")

while(1):
    for url in urls:
        response = requests.request("GET", url, headers=headers, data=payload)
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError as e:
            pass
        
        if(data['m2m:cin']['lbl'][0] == "AE-WE"):
            weather(response.text)
        else:
            con_string = data['m2m:cin']['con']
            con_list = json.loads(con_string)
            timestamp = datetime.utcfromtimestamp(con_list[0]).strftime('%Y-%m-%d %H:%M:%S')
            
            parsed_data = {
            "Timestamp": timestamp,
            "Calibrated PM2.5": con_list[1],
            "PM10": con_list[2],
            "Calibrated PM10": con_list[3],
            "Temperature": con_list[4],
            "Calibrated Temperature": con_list[5],
            "Relative Humidity": con_list[6],
            "Calibrated Relative Humidity": con_list[7],
            "AQI": con_list[8],
            "AQL": con_list[9],
            "AQI-MP": con_list[10],
            "Data Interval": con_list[11]
            }
            parsed_data['eventId'] = generate_short_id()
            parsed_data['latitude'] = '17.5497'
            parsed_data['longitude'] = '78.1230'
            parsed_data['headline'] = gen_description(parsed_data)
            parsed_data['visited'] = False
            parsed_data['dampingFactor'] = 1
            parsed_data['upvotes'] = 0
            parsed_data['downvotes'] = 0
            parsed_data['reports'] = 0
            # Convert dictionary to JSON
            parsed_json = json.dumps(parsed_data, indent=4)

            # Store in file
            filename = f"parsed_sensor_data_{index}.json"
            with open(filename, 'w') as f:
                f.write(parsed_json)

            existing_doc = events.find_one({'headline': parsed_data['headline']})
            if existing_doc is None:
        # If the headline does not exist, insert the document
                events.insert_one(parsed_data)
            # print(f"Data stored in '{filename}' file.")

        index += 1
