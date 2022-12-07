import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np
import sqlite3
import pandas as pd

DATABASE = "/Users/felixweber/Documents/CxHistoricalDataGrabber/records.db"
connection = sqlite3.connect(DATABASE)


def getData() -> pd.DataFrame:
    """Returns a DataFrame with all data from the database."""
    data = pd.read_sql_query("SELECT * FROM records", connection)
    # add 'date' column, converting existing ISO8601 startTimestamp column to date
    data["date"] = pd.to_datetime(data["startTimestamp"]).dt.date  # type: ignore
    # add 30 minute interval column, calculate from existing ISO8601 startTimestamp column, remove date
    data["time"] = pd.to_datetime(data["startTimestamp"]).dt.floor("30min").dt.time  # type: ignore
    return data


def findNumberOfCallsPerDay(data: pd.DataFrame) -> pd.DataFrame:
    """Returns a DataFrame with the number of calls on a day."""
    return data.groupby("date").count()["interactionId"]  # type: ignore


def findNumberOfCallsPerHalfHour(data: pd.DataFrame) -> pd.DataFrame:
    """Returns a DataFrame with the average number of calls for each hour of the day."""
    return data.groupby("time").mean()  # type: ignore


def findNumberOfCallsPerDayPerHalfHour(data: pd.DataFrame) -> pd.DataFrame:
    """Returns a DataFrame with the number of calls per day per hour."""
    return data.groupby(["date", "time"]).count()["interactionId"]  # type: ignore


def filterByDate(data: pd.DataFrame, date: datetime) -> pd.DataFrame:
    """Returns a DataFrame with all data from the database."""
    # convert datetime to date
    date = date.date()  # type: ignore
    return data[data["date"] == date]


def broken_performPredictionAndMakeCoolGraph() -> pd.DataFrame:
    """
    Returns a DataFrame with the FORECAST for the next 30 minutes.
    !!!! THIS is the broken bit !!!!
    """
    data: pd.DataFrame = findNumberOfCallsPerHalfHour(getData())
    data: pd.DataFrame = data.reset_index()
    x: pd.Series[int] = data["time"].apply(
        lambda x: x.hour * 60 + x.minute  # type: ignore
    )  # convert time to minutes
    y: pd.Series[float] = data["interactionTime"]  # count of calls
    # perform quadratic regression
    model: np.poly1d = np.poly1d(np.polyfit(x, y, 2))
    # line visualisation
    polyline = np.linspace(0, 24 * 60, 100)
    plt.scatter(x, y)
    plt.plot(polyline, model(polyline))
    # change x axis _label_ to 24h format, one tick every hour
    plt.xticks(np.arange(0, 24 * 60, 60), np.arange(0, 24, 1))
    # plt.show()

    # create new DataFrame with (predicted) the next day of data
    new_data: pd.DataFrame = pd.DataFrame(
        {
            "time": pd.date_range(
                start="2021-02-26 00:00:00",
                end="2021-02-26 23:30:00",
                freq="30min",
            ).time,
            "interactionTime": model(
                np.arange(0, 24 * 60, 30)
            ),  # predict the next day of data
        }
    )
    # convert time to minutes
    new_data["time"] = new_data["time"].apply(
        lambda x: x.hour * 60 + x.minute  # type: ignore
    )
    # perform quadratic regression
    new_data["interactionTime"] = model(new_data["time"])
    # convert minutes back to time
    # new_data["time"] = new_data["time"].apply(
    #     lambda x: datetime.strptime(str(x), "%M").time()  # type: ignore
    # )
    # plot it in a different colour
    plt.scatter(new_data["time"], new_data["interactionTime"], color="red")
    plt.show()

    return new_data


if __name__ == "__main__":
    print(broken_performPredictionAndMakeCoolGraph())
