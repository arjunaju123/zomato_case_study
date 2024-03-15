#couldnt proceed with async method because of 429 error from the server.

import pandas as pd
import aiohttp
import asyncio
from bs4 import BeautifulSoup
from tqdm import tqdm
from aiohttp.client_exceptions import ClientConnectorError
import logging
import time
import json
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

headers = {
    'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36'
}

# Function to extract latitude and longitude from a URL with retry logic
async def extract_lat_long(session, url, index, pbar, retries=5):
    timeout = aiohttp.ClientTimeout(total=25000)  # Total timeout in seconds

    delay = 2  # Initial retry delay
    for _ in range(retries):
        try:
            async with session.get(url, headers=headers, timeout=timeout) as response:
                #print("response is ",response.status)
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    script_content = soup.find('script', string=lambda text: text and 'window.__PRELOADED_STATE__' in text)
                    if script_content:
                        latitude_start_index = script_content.string.find(r'"latitude\":\"') + len(r'"latitude\":\"')
                        latitude_end_index = script_content.string.find('",', latitude_start_index)
                        latitude = script_content.string[latitude_start_index:latitude_end_index-1]

                        longitude_start_index = script_content.string.find(r'"longitude\":\"') + len(r'"longitude\":\"')
                        longitude_end_index = script_content.string.find('",', longitude_start_index)
                        longitude = script_content.string[longitude_start_index:longitude_end_index-1]

                        pbar.update(1)  # Update progress bar
                        return index, latitude, longitude
                elif response.status == 429:
                    retry_after = response.headers.get('Retry-After')
                    if retry_after:
                        print("inside retry after...")
                        logger.warning(f"Received 429 status code. Waiting for {retry_after} seconds before retrying.")
                        await asyncio.sleep(int(retry_after))
                        continue
                    else:
                        #logger.warning("Received 429 status code. Waiting for a few seconds before retrying.")
                        await asyncio.sleep(delay)
                        delay *= 2  # Exponential backoff
                        continue
        except ClientConnectorError:
            logger.warning(f"Connection error occurred for URL: {url}. Retrying in {delay} seconds...")
            await asyncio.sleep(delay)
            delay *= 2  # Exponential backoff
            continue
    logger.error(f"Failed to retrieve data for URL: {url} after {retries} attempts.")
    return index, None, None


async def fetch_all(urls, pbar, checkpoint_file):
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=20)) as session:  # Limit concurrency
        print("INSIDE FETCH ALL FUNCTION")
        tasks = []
        for index, row in urls.iterrows():
            url = row['URL']
            task = asyncio.create_task(extract_lat_long(session, url, index, pbar))
            tasks.append(task)

            #print("tasks are ",len(tasks))

        results = await asyncio.gather(*tasks)
        print("results length is ",len(results))
        for index, latitude, longitude in results:
            print("####################inside###############")
            urls.at[index, 'Latitude'] = latitude
            urls.at[index, 'Longitude'] = longitude
            # Save checkpoint after processing each URL
            # with open(checkpoint_file, 'w') as f:
            #     json.dump(urls.to_dict(), f)

async def main():
    checkpoint_file = "checkpoint.json"
    if os.path.exists(checkpoint_file) and os.path.getsize(checkpoint_file) > 0:
        # Load checkpoint data if available and not empty
        try:
            with open(checkpoint_file, 'r') as f:
                df = pd.DataFrame.from_dict(json.load(f))
        except json.JSONDecodeError:
            logger.error("Error decoding JSON data from checkpoint file. Starting from scratch.")
            df = pd.read_csv("..\data\combined_data.csv")
    else:
        df = pd.read_csv("..\data\combined_data.csv")

    # Add empty columns for latitude and longitude if not already present
    if 'Latitude' not in df.columns:
        df['Latitude'] = ''
    if 'Longitude' not in df.columns:
        df['Longitude'] = ''

    # Get the number of rows in the DataFrame
    data_rows = len(df)

    # Process URLs
    with tqdm(total=data_rows) as pbar:
        await fetch_all(df, pbar, checkpoint_file)

    # Save the updated DataFrame to a new CSV file
    df.to_csv(r"..\data\restaurant_data_with_lat_long_async.csv", index=False)

# Run the asynchronous process
if __name__ == "__main__":
    asyncio.run(main())
