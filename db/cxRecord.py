from datetime import datetime
from typing import Any


class CxRecord:
    interactionId: str  # interaction ID
    startTimestamp: datetime  # start time of the interaction
    interactionTime: int  # in seconds
    queue: str  # queue ID
    channelType: str  # voice | chat

    @staticmethod
    def fromDict(_dict: dict[str, Any]):
        if "queues" not in _dict:
            raise ValueError("No queue found in record")
        if "interactionTime" not in _dict:
            raise ValueError("No interaction time found in record")

        record = CxRecord()
        record.interactionId = _dict["interactionId"]
        # get UTC timestamp from iso8601
        record.startTimestamp = datetime.fromisoformat(_dict["startTimestamp"][:-5])
        record.interactionTime = _dict["interactionTime"]
        record.queue = _dict["queues"][0]["queueId"]
        record.channelType = _dict["channelType"]
        return record
