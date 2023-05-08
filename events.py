from abc import ABC
from collections import Counter


class Event(ABC):
    type_code = "PLACEHOLDER"

    def __init__(self, event_tuple):
        self.data = {
            "event_id": string,
            "type": string,
            "country": string,
            "long": float, 
            "lat": float,
            "datetime": string,
                "images":  
                    [
                        { 
                            "country": string,
                            "long": float, 
                            "lat": float,
                            "datetime": string,
                            "URLImage": string
                        } , ...
                    ]   
        }
        # TODO
        self.date_time = event_tuple[0]
        self.location = event_tuple[2]
        self.images = []

    def is_valid(news_text):
        pass
    
    def build_event(cls, event_type, event_tuple):
        if event_type == "earthquake":
            return Earthquake(event_tuple)
        
    def to_dict(self):
        return {
            "event_id": string,
            "type": self.type_code,
            "country": self.country,
            "long": self.long, 
            "lat": self.lat,
            "datetime": self.date_time,
                "image_url": self.image_url
        }


class Earthquake(Event):
    type_code = "E"

    def is_valid(news_text):
        counter = Counter(news_text.lower().split())
        if counter["earthquake"] < 2:
            return False
        if counter["magnitude"] < 1:
            return False
        return True


