from datetime import datetime, date
from pydantic import BaseModel, Field

from astropy.coordinates import EarthLocation
import astropy.units as u


class CTANorth:
    """site definition for CTA North. Using MAGIC location right now."""

    def __init__(self):
        self.lat = 28.7619 * u.deg
        self.lon = 18.8900 * u.deg
        self.height = 2200 * u.m
        self.location = EarthLocation(lat=self.lat, lon=self.lon, height=self.height)
        self.name = "CTA North"


class Task(BaseModel):
    task_name: str
    task_options: dict


class Coords(BaseModel):
    raInDeg: float = Field(..., ge=0, lt=360)
    decInDeg: float = Field(..., ge=0, lt=360)


class ScienceAlert(BaseModel):
    coords: Coords
    alert_time: datetime


class ObservationWindowOptions(BaseModel):
    max_zenith_deg: float = Field(..., ge=0, le=90)
    search_range_hours: float = Field(..., ge=0)
    max_sun_altitude_deg: float = Field(-18.0, le=0)
    max_moon_altitude_deg: float = Field(-0.5, le=90)
    precision_minutes: float = Field(..., ge=0)
    min_delay_minutes: float = Field(..., ge=0)
    max_delay_minutes: float = Field(..., ge=0)
    min_duration_minutes: float = Field(..., ge=0)


class FactorialsOptions(BaseModel):
    fact_n: int


class ObservationWindow(BaseModel):
    start_time: datetime
    end_time: datetime
    delay_hours: float
    duration_hours: float


class Night(BaseModel):
    evening_date: date
    sun_set: datetime
    sun_rise: datetime


options_map = {
    "ObservationWindowOptions": ObservationWindowOptions,
    "FactorialsOptions": FactorialsOptions,
}
