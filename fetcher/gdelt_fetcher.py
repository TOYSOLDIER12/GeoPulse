import requests
import zipfile
import io
from datetime import datetime

GDELT_BASE_URL = "http://data.gdeltproject.org/gdeltv2"

def build_gdelt_url(dt: datetime) -> str:
    timestamp = dt.strftime("%Y%m%d%H%M00")
    return f"{GDELT_BASE_URL}/{timestamp}.export.CSV.zip"


def fetch_gdelt_file(dt: datetime) -> io.BytesIO | None:
    url = build_gdelt_url(dt)
    response = requests.get(url, timeout=10)

    if response.status_code != 200:
        return None

    return io.BytesIO(response.content)


def extract_csv(zip_bytes: io.BytesIO) -> io.TextIOBase:
    with zipfile.ZipFile(zip_bytes) as z:
        filename = z.namelist()[0]
        return io.TextIOWrapper(z.open(filename), encoding="utf-8")

