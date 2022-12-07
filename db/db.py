import sqlite3
from typing import Optional
from db.cxRecord import CxRecord


class Database:
    def __init__(self):
        self.db_name = "records.db"
        self.conn = sqlite3.connect(self.db_name)
        self.c = self.conn.cursor()
        self.conn.commit()

    def getAll(self):
        self.c.execute("SELECT * FROM records")
        return self.c.fetchall()

    def getWhere(
        self,
        queueId: Optional[str] = None,
        channelType: Optional[str] = None,
        betweenStart: Optional[str] = None,
        betweenEnd: Optional[str] = None,
    ) -> list[CxRecord]:
        query = "SELECT * FROM records "
        if queueId or channelType or betweenStart or betweenEnd:
            query += "WHERE "
        if queueId:
            query += f"queue='{queueId}'"
        if channelType:
            query += f"channelType='{channelType}'"
        if betweenStart and betweenEnd:
            query += f"startTimestamp BETWEEN '{betweenStart}' AND '{betweenEnd}'"

        self.c.execute(query)
        return self.c.fetchall()

    def close(self):
        self.conn.close()

    # on dispose of the object, close the connection
    def __del__(self):
        self.conn.close()
