import feedparser
import json
import random
import time
import xml.etree.ElementTree as ET
import xml.dom.minidom
from datetime import datetime
from pymongo import MongoClient
import pandas as pd 
import spacy 
import requests 

nlp = spacy.load("en_core_web_sm")
pd.set_option("display.max_rows", 200)
api_key = "d93699398730460e83a8f4e57149829f"

# xml to json
def generate_short_id():
    # Generate a short unique identifier based on current timestamp and random number
    timestamp = int(time.time() * 1000)  # Current timestamp in milliseconds
    rand_num = random.randint(0, 999999)  # Random number between 0 and 999999
    short_id = f"{timestamp}-{rand_num:06d}"  # Combine timestamp and random number
    return short_id

def get_timeline_format(rss_url):
    # if "example1.com" in rss_url:
    #     return "%a, %d %b %Y %H:%M:%S %z"  # Example format 1
    if "http://timesofindia.indiatimes.com/rssfeeds/2647163.cms" in rss_url:
        return "%Y-%m-%dT%H:%M:%S%z"        # Example format 2
    elif "https://news.mongabay.com/feed/?post_type=post&feedtype=bulletpoints&topic=environment" in rss_url:
        return "%d %b %Y %H:%M:%S %z"        # Example format 2
    elif "https://feeds.nbcnews.com/nbcnews/public/news" in rss_url:
        return "%a, %d %b %Y %H:%M:%S %Z"
    else:
        return "%a, %d %b %Y %H:%M:%S %z"

def xml_to_json(xml_file):
    # Parse the XML file
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # Create a dictionary to hold the JSON data
    json_data = {}

    # Iterate through each 'event' element in the XML tree
    events = []
    for event in root.findall(".//event"):
        event_data = {}
        for child in event:
            # Skip 'event' elements with no children
            if child.tag is not None:
                if child.tag == 'urgency':
                    event_data[child.tag] = child.text
                else:
                    event_data[child.tag] = child.text.strip() if child.text else None
        # Add the additional parameters
        if(index == 1):
            event_data['source'] = "https://www.downtoearth.org.in/rss/natural-disasters"
        else:
            event_data['source'] = "http://timesofindia.indiatimes.com/rssfeeds/2647163.cms"
        event_data['eventId'] = generate_short_id()
        event_data['visited'] = False
        event_data['dampingFactor'] = 1
        event_data['upvotes'] = 0
        event_data['downvotes'] = 0
        event_data['reports'] = 0
        events.append(event_data)
    
    # Construct the JSON data
    json_data['events'] = events

    # Convert the dictionary to JSON format
    json_string = json.dumps(json_data, indent=2)
    
    return json_string

def save_json_to_file(json_string, output_file):
    with open(output_file, 'w') as file:
        file.write(json_string)
    
def store(output_file):
    with open(output_file) as file:
        json_data = json.load(file)

    events.insert_one(json_data)

client = MongoClient('mongodb+srv://Developer:Bahubhashak@bahubhashaak-project.ascwu.mongodb.net/EMRI?retryWrites=true&w=majority')
print("Connected successfully!!!")
guardianAngel = client['GuardianAngel']
events = guardianAngel['events']

rss_urls = {
             "http://timesofindia.indiatimes.com/rssfeeds/2647163.cms", 
        #    "https://feeds.feedburner.com/ndtvnews-latest",
            "https://www.downtoearth.org.in/rss/natural-disasters",
        }

while(1):
    index =0
    for rss_url in rss_urls:
        # cap = ET.Element("alert", xmlns="urn:oasis:names:tc:emergency:cap:1.2")
        info = ET.Element("info")

        sender = ET.SubElement(info, "sender")
        sender.text = "Your Organization"

        status = ET.SubElement(info, "status")
        status.text = "Actual"
        
        feed = feedparser.parse(rss_url)
        source = ET.SubElement(info, "source")
        source.text = rss_url  # Set the source URL of the RSS feed

    # Extract relevant information from RSS feed entries and format into CAP XML
        for entry in feed.entries:
            event = ET.SubElement(info, "event")
            title = entry.title
            description = entry.description
            link = entry.link
            # published = entry.published

        # Extract timeline format based on RSS URL
            timeline_format = get_timeline_format(rss_url)
            lat='0'
            lng='0'
            content = description + title
            doc = nlp(content)
            for word in doc.ents:
                print(word.text, word.label_)
                if word.label_ == "GPE" or word.label_ == "LOC":
                    address = word.text
                    url = f"https://api.opencagedata.com/geocode/v1/json?q={address}&key={api_key}"
                    response = requests.get(url)
                    data = response.json()
                    if data['total_results'] > 0:
                        data = response.json()['results'][0]['geometry']
                        lat = str(data['lat'])
                        lng = str(data['lng'])

        # Add information to CAP XML
            ET.SubElement(event, "headline").text = title  # Add headline
            ET.SubElement(event, "description").text = description
            ET.SubElement(event, "urgency").text = "Immediate"
            ET.SubElement(event, "sent").text = datetime.utcnow().isoformat() + "+00:00"  # Set sent time to current UTC time
            ET.SubElement(event, "msgType").text = "Alert"
            ET.SubElement(event,"latitude").text = lat
            ET.SubElement(event,"longitude").text = lng

    # Convert CAP XML to a prettified string
        xml_str = ET.tostring(info, encoding="unicode", method="xml")
        xml_str_prettified = xml.dom.minidom.parseString(xml_str).toprettyxml()
    # 
    # Write prettified XML to file
        new_file = f"rss_{index}.xml"
        with open(new_file, "w") as xml_file:
            xml_file.write(xml_str_prettified)

        print("CAP XML file generated successfully.")
        json_output = xml_to_json(new_file)
        output_file = f"output_{index}.json"
        save_json_to_file(json_output, output_file)
        store(output_file)
        print(f"JSON data saved to {output_file}")
        # insert_xml_file(new_file)
        index += 1

def get_headlines_from_xml(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    headlines = [event.find('headline').text for event in root.findall('event')]
    return headlines

def detect_new_headlines(file_path, previous_headlines):
    current_headlines = set(get_headlines_from_xml(file_path))
    new_headlines = current_headlines - set(previous_headlines)
    return new_headlines

