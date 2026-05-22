from data_tools.collections import TimeSeries
from data_tools.query import InfluxDBClient
from datetime import datetime

if __name__ == "__main__":
    client = InfluxDBClient()

    # ISO 8601-compliant times corresponding to pre-competition testing
    start = datetime.fromisoformat("2024-07-16T18:23:57Z")
    stop = datetime.fromisoformat("2024-07-16T18:34:15Z")

    # We can, in one line, make a query to InfluxDB and parse
    # the data into a powerful format: the `TimeSeries` class.
    voltage_data: TimeSeries = client.query_time_series(
        start=start,
        stop=stop,
        field="TotalPackVoltage",
        units="V"
    )

    # Plot the data
    voltage_data.plot()
