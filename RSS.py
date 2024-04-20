import feedparser
import xml.etree.ElementTree as ET
import xml.dom.minidom
from datetime import datetime
# import pytz
# from sentence_transformers import SentenceTransformer, util
from dbms import insert_xml_file
# Convert time zone to GMT

# Define a function to extract timeline format based on RSS URL
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
        return "%a, %d %b %Y %H:%M:%S %z"  # Default format
from pymongo import MongoClient

# MongoDB connection URL - replace with your actual connection string
MONGO_URI = 'mongodb://localhost:27017'
DATABASE_NAME = 'xml_database'
COLLECTION_NAME = 'urls'
stored_urls = []
def fetch_urls():
    """Fetches URLs from the MongoDB collection and prints them."""
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    try:
        # Fetch all documents within the collection
        urls = collection.find({})
        for doc in urls:
            stored_urls.append(doc.get('url'))
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the MongoDB connection
        client.close()

if __name__ == "__main__":
    fetch_urls()

# Fetch the RSS feed
rss_urls = {
    #politics
            # "https://timesofindia.indiatimes.com/rssfeeds/-2128936835.cms",
            # "https://feeds.feedburner.com/ndtvnews-india-news",
            # "https://www.downtoearth.org.in/rss/india"
            # "https://www.indianewsnetwork.com/rss.en.politics.xml",
            # "https://www.indiatvnews.com/rssnews/topstory-politics.xml"
    # disaster
            #  "http://timesofindia.indiatimes.com/rssfeeds/2647163.cms", 
        #    "https://news.mongabay.com/feed/?post_type=post&feedtype=bulletpoints&topic=environment",
        #    "https://feeds.feedburner.com/ndtvnews-latest",
        #     "https://www.downtoearth.org.in/rss/natural-disasters",
        #     " "
        }

# Create CAP XML structure
index =0
for rss_url in stored_urls:
    cap = ET.Element("alert", xmlns="urn:oasis:names:tc:emergency:cap:1.2")
    info = ET.SubElement(cap, "info")

# Add identifier, sender, status, and source elements
    identifier = ET.SubElement(info, "identifier")
    identifier.text = "123456"  # You can set a unique identifier here

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

    # Extract published time from the entry and convert it to a datetime object
        # published_time = datetime.strptime(published, timeline_format)

    # Add information to CAP XML
        ET.SubElement(event, "headline").text = title  # Add headline
        ET.SubElement(event, "description").text = description
        # ET.SubElement(event, "published").text = published_time.isoformat() + "+00:00"
        ET.SubElement(event, "urgency").text = "Immediate"
        ET.SubElement(event, "sent").text = datetime.utcnow().isoformat() + "+00:00"  # Set sent time to current UTC time
        ET.SubElement(event, "msgType").text = "Alert"
        # ET.SubElement(event, "effective").text = published_time.isoformat() + "+00:00"  # Set effective time to published time
    # Add more elements based on your requirements

# Convert CAP XML to a prettified string
    xml_str = ET.tostring(cap, encoding="unicode", method="xml")
    xml_str_prettified = xml.dom.minidom.parseString(xml_str).toprettyxml()
# 
# Write prettified XML to file
    new_file = f"rss_{index}.xml"
    with open(new_file, "w") as xml_file:
        xml_file.write(xml_str_prettified)

    print("CAP XML file generated successfully.")
    insert_xml_file(new_file)
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

# api to insert new source
# api to fetch CAP
# live data -> election, politics related
# summariser in same headline -> update CAP file
# nrsc, imd etc. 6 sources