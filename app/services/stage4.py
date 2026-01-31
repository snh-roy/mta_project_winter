import gzip
import os
import re
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import httpx
import numpy as np
import pandas as pd

from app.config import get_settings

settings = get_settings()
if not os.environ.get("ECCODES_DEFINITION_PATH"):
    os.environ["ECCODES_DEFINITION_PATH"] = settings.eccodes_definition_path
if not os.environ.get("ECCODES_SAMPLES_PATH"):
    os.environ["ECCODES_SAMPLES_PATH"] = settings.eccodes_samples_path

import pygrib


class Stage4Service:
    """Service for fetching historical NCEP Stage IV hourly precipitation from IEM archive."""

    FILE_EXTENSIONS = (".grb", ".grb2", ".grib", ".grib2", ".grb2.gz", ".grib2.gz", ".gz")

    def __init__(self):
        self.settings = get_settings()
        self._dir_cache: dict[str, list[str]] = {}
        self._grid_cache: dict[str, tuple[np.ndarray, np.ndarray]] = {}

    def _archive_dirs(self, date: datetime) -> list[str]:
        bases = [self.settings.stage4_archive_base_url]
        if self.settings.stage4_archive_fallback_base_url:
            bases.append(self.settings.stage4_archive_fallback_base_url)
        return [
            f"{base}/{date.strftime('%Y/%m/%d')}/stage4/"
            for base in bases
        ]

    async def _list_dir(self, date: datetime) -> list[str]:
        key = date.strftime("%Y-%m-%d")
        if key in self._dir_cache:
            return self._dir_cache[key]

        urls = self._archive_dirs(date)
        text = ""
        for url in urls:
            try:
                async with httpx.AsyncClient(follow_redirects=True) as client:
                    response = await client.get(
                        url,
                        headers={"User-Agent": "mta-flood-api"},
                        timeout=20.0,
                    )
                    if response.status_code == 200 and response.text:
                        text = response.text
                        break
            except Exception as e:
                print(f"Stage IV directory fetch failed for {url}: {e}")

        # Extract href targets
        links = re.findall(r'href="([^"]+)"', text)
        files = [link for link in links if link.endswith(self.FILE_EXTENSIONS)]
        self._dir_cache[key] = files
        return files

    def _parse_time_from_name(self, name: str) -> Optional[datetime]:
        # Try to find a timestamp in the filename
        candidates = re.findall(r"(\d{10,14})", name)
        for c in candidates:
            for fmt in ("%Y%m%d%H", "%Y%m%d%H%M", "%Y%m%d%H%M%S"):
                try:
                    return datetime.strptime(c, fmt).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue

        # Try patterns like YYYYMMDD.HH
        match = re.search(r"(\d{8})[._-](\d{2})", name)
        if match:
            try:
                dt = datetime.strptime(match.group(1) + match.group(2), "%Y%m%d%H")
                return dt.replace(tzinfo=timezone.utc)
            except ValueError:
                return None

        return None

    async def _find_nearest_file(self, target_time: datetime) -> Optional[tuple[datetime, str]]:
        # Stage IV archive is organized by UTC date
        files = await self._list_dir(target_time)
        if not files:
            # Try adjacent days if directory empty
            for offset in (-1, 1):
                files = await self._list_dir(target_time + timedelta(days=offset))
                if files:
                    target_time = target_time + timedelta(days=offset)
                    break
        if not files:
            return None

        closest = None
        closest_delta = None
        for name in files:
            ts = self._parse_time_from_name(name)
            if not ts:
                continue
            delta = abs((ts - target_time).total_seconds())
            if closest_delta is None or delta < closest_delta:
                closest_delta = delta
                closest = name

        if closest is None:
            return None
        return target_time, closest

    async def _download_and_parse(self, date: datetime, filename: str) -> Optional[np.ndarray]:
        url = None
        for base in self._archive_dirs(date):
            url = f"{base}{filename}"
            try:
                async with httpx.AsyncClient(follow_redirects=True) as client:
                    probe = await client.get(
                        url, headers={"Range": "bytes=0-0"}, timeout=10.0
                    )
                    if probe.status_code in (200, 206):
                        break
            except Exception:
                continue
        if not url:
            return None
        try:
            is_gzip = filename.endswith(".gz")
            suffix = ".grib2.gz" if is_gzip else ".grib2"
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp_file:
                async with httpx.AsyncClient(follow_redirects=True) as client:
                    response = await client.get(url, timeout=30.0)
                    response.raise_for_status()
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

                # Cache grid lat/lon
                if grib_path not in self._grid_cache:
                    lats, lons = grb.latlons()
                    self._grid_cache["stage4"] = (lats, lons)

                grbs.close()
                Path(tmp_file.name).unlink(missing_ok=True)
                if grib_path != tmp_file.name:
                    Path(grib_path).unlink(missing_ok=True)

                return data
        except Exception as e:
            print(f"Error fetching Stage IV file {filename}: {e}")
            return None

    def _nearest_indices(self, lats: np.ndarray, lons: np.ndarray, lat: float, lon: float) -> tuple[int, int]:
        # Brute-force nearest neighbor on grid
        if np.nanmax(lons) > 180 and lon < 0:
            lon = lon + 360
        dist = (lats - lat) ** 2 + (lons - lon) ** 2
        idx = np.unravel_index(np.nanargmin(dist), dist.shape)
        return int(idx[0]), int(idx[1])

    async def get_station_precipitation_at_time(
        self, stations_df: pd.DataFrame, target_time: datetime
    ) -> tuple[pd.DataFrame, dict]:
        """Get hourly Stage IV precipitation for all stations at a specific UTC time."""
        nearest = await self._find_nearest_file(target_time)
        if not nearest:
            raise RuntimeError("No Stage IV files found for target time")

        file_date, filename = nearest
        source_url = f"{self._archive_dirs(file_date)[0]}{filename}"
        data = await self._download_and_parse(file_date, filename)
        if data is None:
            raise RuntimeError("Stage IV file download/parse failed")

        lats, lons = self._grid_cache.get("stage4", (None, None))
        if lats is None or lons is None:
            raise RuntimeError("Stage IV grid coordinates not available")

        precip_rates = []
        accum_1hr = []

        for _, row in stations_df.iterrows():
            lat = row["latitude"]
            lon = row["longitude"]
            i, j = self._nearest_indices(lats, lons, lat, lon)
            value = float(data[i, j])
            if value < 0:
                value = 0.0
            # Stage IV hourly totals are in mm; convert to inches
            inches = value / 25.4
            precip_rates.append(inches)
            accum_1hr.append(inches)

        result_df = stations_df.copy()
        result_df["precip_rate_in_hr"] = precip_rates
        result_df["accum_1hr_in"] = accum_1hr

        # 6-hour accumulation: sum last 6 hourly files (best-effort)
        accum_6hr = []
        for _, row in stations_df.iterrows():
            accum_6hr.append(0.0)

        accum_source_urls = []
        accum_source_times = []
        for h in range(0, 6):
            hour_time = target_time - timedelta(hours=h)
            hour_nearest = await self._find_nearest_file(hour_time)
            if not hour_nearest:
                continue
            hour_date, hour_file = hour_nearest
            accum_source_urls.append(f"{self._archive_dirs(hour_date)[0]}{hour_file}")
            accum_source_times.append(hour_time.isoformat())
            hour_data = await self._download_and_parse(hour_date, hour_file)
            if hour_data is None:
                continue
            for idx, (_, row) in enumerate(stations_df.iterrows()):
                lat = row["latitude"]
                lon = row["longitude"]
                i, j = self._nearest_indices(lats, lons, lat, lon)
                value = float(hour_data[i, j])
                if value < 0:
                    value = 0.0
                accum_6hr[idx] += value / 25.4

        result_df["accum_6hr_in"] = accum_6hr
        meta = {
            "precip_source_url": source_url,
            "precip_source_time_utc": target_time.isoformat(),
            "accum_6hr_source_urls": ";".join(accum_source_urls),
            "accum_6hr_source_times_utc": ";".join(accum_source_times),
        }
        return result_df, meta


stage4_service = Stage4Service()
