
from bs4 import BeautifulSoup
import requests
import json

# Function to extract latitude and longitude from script content
def extract_lat_long(script_content):
    # Find script tag containing latitude and longitude
    script_tag = script_content.find('script', string=lambda text: text and 'window.__PRELOADED_STATE__' in text)
    print("script_tag is ",script_tag)
    if script_tag:
        # Extracting the JSON string containing latitude and longitude
        start_index = script_tag.string.find(r'"latitude\":\"')+len(r'"latitude\":\"')
        print("start_index: ",start_index)
        end_index = script_tag.string.find('",', start_index)
        print("end_index: ", end_index)
        latitude = script_tag.string[start_index:end_index-1]

        start_index = script_tag.string.find(r'\"longitude\":\"')+len(r'\"longitude\":\"')
        end_index = script_tag.string.find('",', start_index)
        longitude = script_tag.string[start_index:end_index-1]

        return latitude, longitude
    return None, None
# Sample URL
url = "https://www.zomato.com/agra/the-salt-cafe-kitchen-bar-tajganj/info"

header = {'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36'}

# Make request
response = requests.get(url,headers=header)
print("response is",response)

if response.status_code == 200:
    # Parse HTML content with BeautifulSoup
    soup = BeautifulSoup(response.text, 'html.parser')
    print("soup is",soup)

    # Extract latitude and longitude
    latitude, longitude = extract_lat_long(soup)
    print("Latitude:", latitude)
    print("Longitude:", longitude)
else:
    print("Failed to fetch URL:", response.status_code)


