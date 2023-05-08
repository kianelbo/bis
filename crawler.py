import os
import urllib.request
from datetime import datetime, timedelta
from urllib.error import HTTPError
from time import sleep
from zipfile import ZipFile

import pandas as pd
import requests

from events import Event


headers = [
    "GKGRECORDID", "date", "V2SOURCECOLLECTIONIDENTIFIER", "V2SOURCECOMMONNAME", "V2DOCUMENTIDENTIFIER",
    "V1COUNTS", "V21COUNTS", "tags", "V2ENHANCEDTHEMES", "location", "V2ENHANCEDLOCATIONS",
    "V1PERSONS", "V2ENHANCEDPERSONS", "V1ORGANIZATIONS", "V2ENHANCEDORGANIZATIONS", "V1TONE", "V21ENHANCEDDATES",
    "V2GCAM", "image_url", "V21RELATEDIMAGES", "V21SOCIALIMAGEEMBEDS", "V21SOCIALVIDEOEMBEDS", "V21QUOTATIONS",
    "V21ALLNAMES", "V21AMOUNTS", "V21TRANSLATIONINFO", "V2EXTRASXML",
]

events = {
    "flood": "NATURAL_DISASTER_FLOOD|NATURAL_DISASTER_FLASH_FLOOD",
    "fire": "DISASTER_FIRE|NATURAL_DISASTER_WILDFIRE|NATURAL_DISASTER_FOREST_FIRE|NATURAL_DISASTER_BUSHFIRE|NATURAL_DISASTER_BRUSH_FIRE|NATURAL_DISASTER_GRASS_FIRE",
    "drought": "NATURAL_DISASTER_DROUGHT",
    "earthquake": "NATURAL_DISASTER_EARTHQUAKE",
    "storm": "NATURAL_DISASTER_STORM|NATURAL_DISASTER_TROPICAL_STORM|NATURAL_DISASTER_WINTER_STORM|NATURAL_DISASTER_SNOWSTORM|NATURAL_DISASTER_WINDSTORM",
    "tsunami": "NATURAL_DISASTER_TSUNAMI",
    "volcanic_eruption": "NATURAL_DISASTER_VOLCAN",
    "oil_spill": "MANMADE_DISASTER_OIL_SPILL",
    "blackout": "MANMADE_DISASTER_POWER_BLACKOUT",
}


class Crawler:
    def __init__(self, storage, initial_date) -> None:
        self.storage = storage
        self.last_time = self._truncate_dt(initial_date) - timedelta(minutes=15)

    def run(self):
        while True:
            current_time = self._truncate_dt(datetime.now())
            if current_time > self.last_time:
                dt_string = current_time.strftime("%Y%m%d%H%M")
                new_file_available = self.download_csv_file(dt_string)
                if not new_file_available:
                    sleep(60 * 5)
                    continue

                csv_path = "tmp/{dt_string}00.gkg.csv', sep='\t"
                df = pd.read_csv(csv_path, low_memory=False)
                os.remove(csv_path)

                df.columns = headers
                df = df[["date", "tags", "location", "image_url", "source_url"]]

                self.store_events(df)
                print(f"Saved the data of {dt_string}")
            else:
                sleep(60 * 5)

    def download_csv_file(self, dt_string):
        url = f"http://data.gdeltproject.org/gdeltv2/{dt_string}00.gkg.csv.zip"
        file_path = f"tmp/{dt_string}.csv.zip"
        try:
            urllib.request.urlretrieve(url, file_path)
        except HTTPError as err:
            # if we see 404 it means that new file is not available
            if err.code == 404:
                return False
        with ZipFile(file_path, 'r') as zipped_file:
            zipped_file.extractall("data")

        os.remove(file_path)
        return True

    def store_events(self, df):
        for event_type, tag in events.items():
            event_df = df[df["tags"].str.contains(tag, na=False)]
            events = event_df[event_df["location"].notna()].values.tolist()

            for event in events:
                event_obj = Event.build_event(event_type, event)
                page = requests.get(event[4])
                if not event_obj.is_valid(page.content):
                    continue
                self.storage.insert(event_obj)

    def _truncate_dt(dt):
        delta = timedelta(minutes=15)
        return datetime.min + round((dt - datetime.min) / delta) * delta
