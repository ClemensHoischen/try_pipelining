from datetime import datetime
from pydantic import BaseModel, Field
from typing import List

from astropy.coordinates import EarthLocation
import astropy.units as u


# ---------- General structs --------------------------


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
    filter_options: dict


class Coords(BaseModel):
    raInDeg: float = Field(..., ge=0, lt=360)
    decInDeg: float = Field(..., ge=0, lt=360)


class ScienceAlert(BaseModel):
    coords: Coords
    alert_time: datetime


# ---------------- Option Structs ---------------


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


# --------------- Filter Option structs ----------


class FactorialsFilterOptions(BaseModel):
    min_fact_val: float = Field(..., ge=0)


class ObservationWindowFilterOptions(BaseModel):
    min_window_duration_hours: float = Field(..., ge=0)
    max_window_delay_hours: float = Field(..., ge=0)


# -------------- Output structs -----------------


class FactorialsTaskResult(BaseModel):
    factorial_result: float


class ObservationWindow(BaseModel):
    start_time: datetime
    end_time: datetime
    delay_hours: float
    duration_hours: float


class ObservationWindowTaskResult(BaseModel):
    windows: List[ObservationWindow]


# ------------- Option Mapping ----------------------

options_map = {
    "ObservationWindowPipeline": ObservationWindowOptions,
    "FactorialsPipeline": FactorialsOptions,
}

filter_option_map = {
    "ObservationWindowPipeline": ObservationWindowFilterOptions,
    "FactorialsPipeline": FactorialsFilterOptions,
}