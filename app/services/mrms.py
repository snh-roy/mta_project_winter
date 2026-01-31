import gzip
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import boto3
import httpx
import numpy as np
import pandas as pd
from botocore import UNSIGNED
from botocore.config import Config

from app.config import get_settings

settings = get_settings()
if not os.environ.get("ECCODES_DEFINITION_PATH"):
    os.environ["ECCODES_DEFINITION_PATH"] = settings.eccodes_definition_path
if not os.environ.get("ECCODES_SAMPLES_PATH"):
    os.environ["ECCODES_SAMPLES_PATH"] = settings.eccodes_samples_path

import pygrib


class MRMSService:
    """Service for fetching NOAA MRMS precipitation data from AWS S3 bucket"""

    # MRMS grid parameters for CONUS
    # Grid is 7000x3500 cells, 0.01 degree resolution
    MRMS_LAT_MIN = 20.0
    MRMS_LAT_MAX = 55.0
    MRMS_LON_MIN = -130.0
    MRMS_LON_MAX = -60.0
    MRMS_RESOLUTION = 0.01

    # Products we need
    PRODUCTS = {
        "precip_rate": "PrecipRate",
        "qpe_01h": "GaugeCorr_QPE_01H",
        "qpe_06h": "GaugeCorr_QPE_06H",
    }

    HTTP_PRODUCTS = {
        "precip_rate": "PrecipRate",
        "qpe_01h": "MultiSensor_QPE_01H_Pass2",
        "qpe_06h": "MultiSensor_QPE_06H_Pass2",
    }

    HTTP_INTERVAL_MINUTES = {
        "precip_rate": 2,
        "qpe_01h": 60,
        "qpe_06h": 60,
    }

    def __init__(self):
        self.settings = get_settings()
        if not os.environ.get("ECCODES_DEFINITION_PATH"):
            os.environ["ECCODES_DEFINITION_PATH"] = self.settings.eccodes_definition_path
        if not os.environ.get("ECCODES_SAMPLES_PATH"):
            os.environ["ECCODES_SAMPLES_PATH"] = self.settings.eccodes_samples_path
        self._s3_client = None
        self._precip_cache: dict = {}
        self._cache_time: Optional[datetime] = None
        self._cache_ttl = timedelta(minutes=5)

    @property
    def s3_client(self):
        """Get S3 client configured for anonymous access."""
        if self._s3_client is None:
            self._s3_client = boto3.client(
                "s3",
                region_name=self.settings.mrms_region,
                config=Config(signature_version=UNSIGNED),
            )
        return self._s3_client

    def _is_cache_valid(self) -> bool:
        """Check if cached data is still valid."""
        if self._cache_time is None:
            return False
        return datetime.now(timezone.utc) - self._cache_time < self._cache_ttl

    async def get_latest_file_key(self, product: str) -> Optional[str]:
        """Get the key of the most recent MRMS file for a product."""
        # MRMS files are organized by product/MRMS_{product}_{level}_{timestamp}.grib2.gz
        prefix = f"CONUS/{product}/"

        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.settings.mrms_bucket,
                Prefix=prefix,
                MaxKeys=100,
            )

            if "Contents" not in response:
                return None

            # Sort by LastModified to get most recent
            files = sorted(
                response["Contents"],
                key=lambda x: x["LastModified"],
                reverse=True,
            )

            # Return most recent .grib2.gz file
            for f in files:
                if f["Key"].endswith(".grib2.gz"):
                    return f["Key"]

            return None

        except Exception as e:
            print(f"Error listing MRMS files: {e}")
            return None

    async def download_and_parse_grib(self, key: str) -> Optional[np.ndarray]:
        """Download a GRIB2 file from S3 and parse it."""
        try:
            with tempfile.NamedTemporaryFile(suffix=".grib2.gz", delete=False) as tmp_gz:
                self.s3_client.download_file(
                    self.settings.mrms_bucket,
                    key,
                    tmp_gz.name,
                )

                # Decompress
                grib_path = tmp_gz.name.replace(".gz", "")
                with gzip.open(tmp_gz.name, "rb") as gz_file:
                    with open(grib_path, "wb") as grib_file:
                        grib_file.write(gz_file.read())

                # Parse GRIB2
                grbs = pygrib.open(grib_path)
                grb = grbs[1]  # First message
                data = grb.values

                # Clean up
                grbs.close()
                Path(tmp_gz.name).unlink(missing_ok=True)
                Path(grib_path).unlink(missing_ok=True)

                return data

        except Exception as e:
            print(f"Error downloading/parsing GRIB: {e}")
            return None

    async def download_and_parse_grib_http(self, url: str) -> Optional[np.ndarray]:
        """Download a GRIB2 file over HTTP and parse it."""
        try:
            is_gzip = url.endswith(".gz")
            suffix = ".grib2.gz" if is_gzip else ".grib2"
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp_file:
                async with httpx.AsyncClient(follow_redirects=True) as client:
                    response = await client.get(url, timeout=30.0)
                    response.raise_for_status()
                    content_type = response.headers.get("Content-Type", "")
                    if "gzip" not in content_type and "octet-stream" not in content_type:
                        return None
                    tmp_file.write(response.content)

                if is_gzip:
                    grib_path = tmp_file.name.replace(".gz", "")
                    with gzip.open(tmp_file.name, "rb") as gz_file:
                        with open(grib_path, "wb") as grib_file:
                            grib_file.write(gz_file.read())
                else:
                    grib_path = tmp_file.name

                grbs = pygrib.open(grib_path)
                grb = grbs[1]
                data = grb.values

                grbs.close()
                Path(tmp_file.name).unlink(missing_ok=True)
                if grib_path != tmp_file.name:
                    Path(grib_path).unlink(missing_ok=True)

                return data
        except Exception as e:
            print(f"Error downloading/parsing GRIB via HTTP: {e}")
            return None

    def _build_http_url(self, product: str, timestamp: datetime) -> str:
        ts = timestamp.strftime("%Y%m%d-%H%M%S")
        return (
            f"{self.settings.mrms_http_base_url}/{product}/"
            f"MRMS_{product}_00.00_{ts}.grib2.gz"
        )

    def _build_archive_url(self, product: str, timestamp: datetime) -> str:
        ts = timestamp.strftime("%Y%m%d-%H%M%S")
        return (
            f"{self.settings.mrms_archive_base_url}/"
            f"{timestamp.strftime('%Y/%m/%d')}/mrms/ncep/{product}/"
            f"{product}_00.00_{ts}.grib2.gz"
        )

    async def _find_nearest_http_file(
        self,
        product_key: str,
        target_time: datetime,
        window_minutes: int = 360,
        base_source: str = "realtime",
    ) -> Optional[str]:
        """Find nearest available MRMS HTTP file within window."""
        step = self.HTTP_INTERVAL_MINUTES.get(product_key, 2)
        base_time = target_time.replace(second=0, microsecond=0)
        offsets = [0]
        for i in range(1, window_minutes // step + 1):
            offsets.extend([-i * step, i * step])

        http_product = self.HTTP_PRODUCTS.get(product_key)
        if not http_product:
            return None

        async with httpx.AsyncClient(follow_redirects=True) as client:
            for offset in offsets:
                candidate = base_time + timedelta(minutes=offset)
                if base_source == "archive":
                    url = self._build_archive_url(http_product, candidate)
                else:
                    url = self._build_http_url(http_product, candidate)
                try:
                    if base_source == "archive":
                        probe = await client.get(
                            url, headers={"Range": "bytes=0-0"}, timeout=15.0
                        )
                        if probe.status_code in (200, 206):
                            return url
                    else:
                        head = await client.head(url, timeout=10.0)
                        if head.status_code == 200:
                            return url
                except Exception:
                    continue

        return None

    def _latlon_to_grid_index(self, lat: float, lon: float) -> tuple[int, int]:
        """Convert lat/lon to MRMS grid indices."""
        # MRMS grid is north-up, so we need to flip latitude
        lat_idx = int((self.MRMS_LAT_MAX - lat) / self.MRMS_RESOLUTION)
        lon_idx = int((lon - self.MRMS_LON_MIN) / self.MRMS_RESOLUTION)

        # Clamp to valid range
        lat_idx = max(0, min(lat_idx, 3499))
        lon_idx = max(0, min(lon_idx, 6999))

        return lat_idx, lon_idx

    def get_value_at_location(
        self, data: np.ndarray, lat: float, lon: float
    ) -> Optional[float]:
        """Extract precipitation value at a specific lat/lon."""
        if data is None:
            return None

        lat_idx, lon_idx = self._latlon_to_grid_index(lat, lon)

        try:
            value = float(data[lat_idx, lon_idx])
            # MRMS uses large negative values for missing data
            if value < 0 or value > 1000:
                return 0.0
            return value
        except (IndexError, ValueError):
            return None

    async def fetch_precipitation_data(
        self, force_refresh: bool = False
    ) -> dict[str, Optional[np.ndarray]]:
        """Fetch all precipitation products from MRMS."""
        if not force_refresh and self._is_cache_valid():
            return self._precip_cache

        result = {}

        for product_key, product_name in self.PRODUCTS.items():
            file_key = await self.get_latest_file_key(product_name)
            if file_key:
                data = await self.download_and_parse_grib(file_key)
                result[product_key] = data
                continue

            # Fallback to HTTP latest file if S3 listing fails
            http_product = self.HTTP_PRODUCTS.get(product_key)
            if http_product:
                url = f"{self.settings.mrms_http_base_url}/{http_product}/MRMS_{http_product}.latest.grib2.gz"
                data = await self.download_and_parse_grib_http(url)
                result[product_key] = data
            else:
                result[product_key] = None

        self._precip_cache = result
        self._cache_time = datetime.now(timezone.utc)

        return result

    async def fetch_precipitation_data_at_time(
        self, target_time: datetime
    ) -> dict[str, Optional[np.ndarray]]:
        """Fetch MRMS precipitation products closest to a target UTC time via HTTP."""
        result: dict[str, Optional[np.ndarray]] = {}
        for product_key in self.HTTP_PRODUCTS.keys():
            url = await self._find_nearest_http_file(product_key, target_time, base_source="archive")
            if url:
                data = await self.download_and_parse_grib_http(url)
                result[product_key] = data
            else:
                result[product_key] = None
        return result

    async def get_station_precipitation_at_time(
        self, stations_df: pd.DataFrame, target_time: datetime
    ) -> pd.DataFrame:
        """Get precipitation data for all stations at a specific time (UTC)."""
        precip_data = await self.fetch_precipitation_data_at_time(target_time)

        precip_rates = []
        accum_1hr = []
        accum_6hr = []

        for _, row in stations_df.iterrows():
            lat = row["latitude"]
            lon = row["longitude"]

            rate = self.get_value_at_location(precip_data.get("precip_rate"), lat, lon)
            qpe_1h = self.get_value_at_location(precip_data.get("qpe_01h"), lat, lon)
            qpe_6h = self.get_value_at_location(precip_data.get("qpe_06h"), lat, lon)

            precip_rates.append(rate / 25.4 if rate else 0.0)
            accum_1hr.append(qpe_1h / 25.4 if qpe_1h else 0.0)
            accum_6hr.append(qpe_6h / 25.4 if qpe_6h else 0.0)

        result_df = stations_df.copy()
        result_df["precip_rate_in_hr"] = precip_rates
        result_df["accum_1hr_in"] = accum_1hr
        result_df["accum_6hr_in"] = accum_6hr

        return result_df

    async def get_station_precipitation(
        self, stations_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Get precipitation data for all stations."""
        precip_data = await self.fetch_precipitation_data()

        precip_rates = []
        accum_1hr = []
        accum_6hr = []

        for _, row in stations_df.iterrows():
            lat = row["latitude"]
            lon = row["longitude"]

            rate = self.get_value_at_location(precip_data.get("precip_rate"), lat, lon)
            qpe_1h = self.get_value_at_location(precip_data.get("qpe_01h"), lat, lon)
            qpe_6h = self.get_value_at_location(precip_data.get("qpe_06h"), lat, lon)

            # Convert from mm/hr to in/hr and mm to inches
            precip_rates.append(rate / 25.4 if rate else 0.0)
            accum_1hr.append(qpe_1h / 25.4 if qpe_1h else 0.0)
            accum_6hr.append(qpe_6h / 25.4 if qpe_6h else 0.0)

        result_df = stations_df.copy()
        result_df["precip_rate_in_hr"] = precip_rates
        result_df["accum_1hr_in"] = accum_1hr
        result_df["accum_6hr_in"] = accum_6hr

        return result_df

    async def get_single_station_precipitation(
        self, lat: float, lon: float
    ) -> dict[str, Optional[float]]:
        """Get precipitation data for a single station."""
        precip_data = await self.fetch_precipitation_data()

        rate = self.get_value_at_location(precip_data.get("precip_rate"), lat, lon)
        qpe_1h = self.get_value_at_location(precip_data.get("qpe_01h"), lat, lon)
        qpe_6h = self.get_value_at_location(precip_data.get("qpe_06h"), lat, lon)

        return {
            "precip_rate_in_hr": rate / 25.4 if rate else 0.0,
            "accum_1hr_in": qpe_1h / 25.4 if qpe_1h else 0.0,
            "accum_6hr_in": qpe_6h / 25.4 if qpe_6h else 0.0,
        }

    async def is_available(self) -> bool:
        """Check if MRMS data is available."""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.settings.mrms_bucket,
                Prefix="CONUS/PrecipRate/",
                MaxKeys=1,
            )
            if "Contents" in response:
                return True
        except Exception:
            pass

        # Fallback to HTTP "latest" file availability
        http_product = self.HTTP_PRODUCTS.get("precip_rate")
        if not http_product:
            return False

        url = f"{self.settings.mrms_http_base_url}/{http_product}/MRMS_{http_product}.latest.grib2.gz"
        try:
            async with httpx.AsyncClient(follow_redirects=True) as client:
                head = await client.head(url, timeout=10.0)
                if head.status_code == 200:
                    return True
                response = await client.get(url, timeout=10.0)
                return response.status_code == 200
        except Exception:
            return False


mrms_service = MRMSService()
