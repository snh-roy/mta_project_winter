from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # API Settings
    app_name: str = "MTA Flood Risk Monitoring API"
    app_version: str = "1.0.0"
    debug: bool = False

    # MTA Station Data
    mta_stations_url: str = "http://web.mta.info/developers/data/nyct/subway/Stations.csv"
    stations_cache_path: str = "app/data/MTA_Subway_Stations.csv"

    # NOAA MRMS S3 Bucket
    mrms_bucket: str = "noaa-mrms-pds"
    mrms_region: str = "us-east-1"
    mrms_http_base_url: str = "https://mrms.ncep.noaa.gov/2D"
    mrms_archive_base_url: str = "https://mtarchive.geol.iastate.edu/thredds/fileServer/mtarchive"
    stage4_archive_base_url: str = "https://mesonet.agron.iastate.edu/archive/data"
    stage4_archive_fallback_base_url: str = "https://mesonet2.agron.iastate.edu/archive/data"

    # ECCODES paths for GRIB decoding (pygrib)
    eccodes_definition_path: str = "/opt/anaconda3/share/eccodes/definitions"
    eccodes_samples_path: str = "/opt/anaconda3/share/eccodes/samples"

    # NOAA Tides & Currents API
    noaa_tides_base_url: str = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
    noaa_battery_station: str = "8518750"
    noaa_kings_point_station: str = "8516945"

    # NOAA/NCEI CDO (GHCN-Daily)
    ncei_cdo_base_url: str = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
    ncei_cdo_token: str = ""
    ghcnd_central_park_station: str = "GHCND:USW00094728"
    ghcnd_jfk_station: str = "GHCND:USW00094789"
    ghcnd_lga_station: str = "GHCND:USW00014732"

    # NWS API viewed via gridpoint forecast data
    nws_base_url: str = "https://api.weather.gov"

    # USGS Water Data API
    usgs_water_url: str = "https://waterservices.usgs.gov/nwis/iv/"
    usgs_sites: str = "01302050,01311145,01311143"

    # Risk Thresholds - Subway (Underground)
    subway_high_precip_rate: float = 0.5
    subway_high_accum_6hr: float = 2.0
    subway_atrisk_precip_rate: float = 0.25
    subway_atrisk_accum_6hr: float = 1.0

    # Risk Thresholds - Open Cut
    opencut_high_precip_rate: float = 0.75
    opencut_high_accum_6hr: float = 2.5
    opencut_atrisk_precip_rate: float = 0.4
    opencut_atrisk_accum_6hr: float = 1.5

    # Risk Thresholds - Elevated
    elevated_atrisk_precip_rate: float = 1.5

    # Risk Thresholds - Coastal/Tide
    tide_high_level: float = 5.0
    coastal_high_precip_rate: float = 0.3
    coastal_atrisk_precip_rate: float = 0.15

    # Default thresholds
    default_high_precip_rate: float = 0.75
    default_high_accum_6hr: float = 2.5
    default_atrisk_precip_rate: float = 0.4
    default_atrisk_accum_6hr: float = 1.5

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    def validate_required(self) -> list[str]:
        missing = []
        if not self.ncei_cdo_token:
            missing.append("NCEI_CDO_TOKEN")
        if not self.eccodes_definition_path or not self.eccodes_samples_path:
            missing.append("ECCODES_DEFINITION_PATH / ECCODES_SAMPLES_PATH")
        return missing


# Coastal stations that should have tide data applied
COASTAL_STATIONS = [
    "Broad Channel",
    "Howard Beach-JFK Airport",
    "Rockaway Park-Beach 116 St",
    "Beach 67 St",
    "Beach 60 St",
    "Beach 44 St",
    "Beach 36 St",
    "Beach 25 St",
    "Far Rockaway-Mott Av",
    "South Ferry",
    "Whitehall St-South Ferry",
    "Coney Island-Stillwell Av",
]

# Valid boroughs
VALID_BOROUGHS = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]


@lru_cache
def get_settings() -> Settings:
    return Settings()
