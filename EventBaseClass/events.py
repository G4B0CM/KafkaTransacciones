# events.py

from dataclasses import dataclass
import json
import datetime

@dataclass
class PurchaseEvent:
    user_id: str
    product_id: str
    amount: float
    status: str 
    timestamp: str

    def to_json(self):
        return json.dumps(self.__dict__)

    @staticmethod
    def from_json(data):
        return PurchaseEvent(**json.loads(data))
