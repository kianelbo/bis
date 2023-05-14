import os
import urllib.request
from datetime import datetime, timedelta
from glob import glob
from pathlib import Path
from urllib.error import HTTPError
from zipfile import ZipFile

import pandas as pd


headers = [
    "GKGRECORDID", "date", "V2SOURCECOLLECTIONIDENTIFIER", "V2SOURCECOMMONNAME", "source_url",
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

country_codes = []
with open("countries.txt", "r") as contries_file:
    country_codes = [line.rstrip() for line in contries_file]


class Crawler:
    def __init__(self, storage, articles_sieve, vectorizer, start_date=None, tmp_dir="tmp2"):
        self.storage = storage
        self.articles_sieve = articles_sieve
        self.vectorizer = vectorizer
        if start_date == None:
            self.start_date = datetime(day=6, month=2, year=2023)
        else:
            self.start_date = self._truncate_dt(start_date) - timedelta(minutes=15)
        self.tmp_dir = tmp_dir
        Path(self.tmp_dir).mkdir(exist_ok=True)

    def run(self, skip_download=False):
        if not skip_download:
            self.download_csv_files(self.start_date)
            print(f"CSV files of {self.start_date.date().isoformat()} downloaded")

        df = self.build_df()
        print("Events dataframe created")
        df = self.clean_df(df)
        print("Preprocessing dataframe completed")

        # only turkey earthquake :(
        raw_events = df[df["tags"].str.contains("NATURAL_DISASTER_EARTHQUAKE", na=False)].values.tolist()
        target_countries = self.get_target_countries(raw_events)
        print("Analyzing target locations completed")

        data = self.extract_events_data(raw_events, target_countries, "earthquake", self.start_date)
        print(f"Output data serialized. Total events count: {len(data)}")
        self.store_events(data)
        print("Successfully stored on the database")

    def get_target_countries(self, raw_events, top_k=2):
        # TODO: top_k should be handled better
        location_freq = {code: 0 for code in country_codes}
        for event in raw_events:
            for loc in event[2].split(';'):
                country_code = loc.split('#')[2]
                if country_code in location_freq:
                    location_freq[country_code] += 1
        return [lf[0] for lf in sorted(location_freq.items(), key=lambda i: i[1], reverse=True)][:top_k]

    def extract_events_data(self, raw_events, target_countries, event_type, date):
        if not isinstance(date, str):
            date = date.date().isoformat()
        data = {
            country: {
                "event_id": f"{event_type}-{country}-{date}", "type": event_type, "country": country, "date": date, "locations": set(), "images": set(),
            }
            for country in target_countries
        }

        for event in raw_events:
            for loc in event[2].split(';'):
                loc_details = loc.split('#')
                country_code = loc_details[2]
                if country_code not in target_countries:
                    continue
                coordinates = (loc_details[4], loc_details[5],)
                data[country_code]["locations"].add(coordinates)

                if event[3]:
                    date_str = self._gdelt_ts_to_str(str(event[0]))
                    dated_image = (date_str, event[3],)
                    data[country_code]["images"].add(dated_image)

        for c in data:
            data[c]["locations"] = list(data[c]["locations"])
            data[c]["images"] = [{"date": img[0], "url": img[1]} for img in data[c]["images"]]

        return list(data.values())

    def download_csv_files(self, d):
        until_date = d + timedelta(days=1)
        while d < until_date:
            dt_string = d.strftime("%Y%m%d%H%M")
            url = f"http://data.gdeltproject.org/gdeltv2/{dt_string}00.gkg.csv.zip"
            file_path = f"{self.tmp_dir}/{dt_string}.csv.zip"
            try:
                urllib.request.urlretrieve(url, file_path)
            except HTTPError as err:
                print(err)
                continue
            with ZipFile(file_path, 'r') as zipped_file:
                zipped_file.extractall(self.tmp_dir)
            os.remove(file_path)
            d += timedelta(minutes=15)

    def build_df(self, cleanup=False):
        chunks = []
        for filename in glob(f"{self.tmp_dir}/*.gkg.csv"):
            chunks.append(pd.read_csv(filename, sep='\t', low_memory=False, encoding="ISO-8859-1", header=None))
            if cleanup:
                os.remove(filename)
        df = pd.concat(chunks, ignore_index=True)
        df.columns = headers
        return df[["date", "tags", "location", "image_url", "source_url"]]

    def clean_df(self, df):
        cleaned_df = df[df["location"].notna()]
        cleaned_df["image_url"] = cleaned_df["image_url"].fillna("")
        return cleaned_df[cleaned_df.apply(lambda row: self._is_relevant(row), axis=1)]

    def store_events(self, data):
        self.storage.insert(data)

    def _is_relevant(self, row):
        tokens = row["source_url"].split("/")
        title = max(tokens, key=len).replace("-", " ")
        vec = self.vectorizer.transform([title])
        return self.articles_sieve.predict(vec)[0] == 1

    @staticmethod
    def _truncate_dt(dt):
        delta = timedelta(minutes=15)
        return datetime.min + round((dt - datetime.min) / delta) * delta

    @staticmethod
    def _gdelt_ts_to_str(ts):
        return f"{ts[:4]}-{ts[4:6]}-{ts[6:8]} {ts[8:10]}:{ts[10:12]}"
