import pyarrow as pa
import pyarrow.flight
import pandas as pd

try:
    client = pa.flight.connect("grpc://localhost:8818")
except Exception as e:
    print(f"Failed to connect to the server: {e}")
    exit(1)

try:
    upload_descriptor = pyarrow.flight.FlightDescriptor.for_path("ons/cpi.parquet")
    resource_flight = client.get_flight_info(upload_descriptor)
except Exception as e:
    print(f"Failed to get flight info: {e}")
    exit(1)

try:
    reader = client.do_get(resource_flight.endpoints[0].ticket)
    read_table = reader.read_all()
    print(read_table.to_pandas().head())
except Exception as e:
    print(f"Failed to read data: {e}")
    exit(1)
