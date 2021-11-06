from datetime import datetime
from typing import Any, List

import astropy.units as u
from astropy.coordinates import EarthLocation
from pydantic import BaseModel, Field

from try_pipelining.observation_windows import ObservationWindow

# ---------- General structs --------------------------


class CTANorth:
    """site definition for CTA North. Using MAGIC location right now."""

    def __init__(self):
        self.lat = 28.7619 * u.deg
        self.lon = 18.8900 * u.deg
        self.height = 2200 * u.m
        self.location = EarthLocation(lat=self.lat, lon=self.lon, height=self.height)
        self.name = "CTA North"


class TaskConfig(BaseModel):
    task_name: str
    task_type: str
    task_options: dict = {}
    filter_options: dict


class Coords(BaseModel):
    raInDeg: float = Field(..., ge=0, lt=360)
    decInDeg: float = Field(..., ge=0, lt=360)


class ScienceAlert(BaseModel):
    coords: Coords
    alert_time: datetime
    measured_parameters: dict


class TimeConstraints(BaseModel):
    start_time: datetime
    end_time: datetime


class WobbleOptions(BaseModel):
    offsets: List[float]
    angles: List[float]


class Proposal(BaseModel):
    proposal_id: int
    proposal_class: str
    proposal_rank: float


class SchedulingBlock(BaseModel):
    coords: Coords
    time_constraints: TimeConstraints
    wobble_options: WobbleOptions


# ---------------- Option Structs ---------------

task_options = {}


def register_task_options(cls):
    task_options.update({cls.__name__.replace("Options", "Task"): cls})
    return cls


@register_task_options
class ObservationWindowOptions(BaseModel):
    max_zenith_deg: float = Field(..., ge=0, le=90)
    search_range_hours: float = Field(..., ge=0)
    max_sun_altitude_deg: float = Field(-18.0, le=0)
    max_moon_altitude_deg: float = Field(-0.5, le=90)
    precision_minutes: float = Field(..., ge=0)
    min_delay_minutes: float = Field(..., ge=0)
    max_delay_minutes: float = Field(..., ge=0)
    min_duration_minutes: float = Field(..., ge=0)


@register_task_options
class FactorialsOptions(BaseModel):
    fact_n: int


@register_task_options
class ParameterOptions(BaseModel):
    pass


# --------------- Filter Option structs ----------

filter_options = {}


def register_filter_options(cls):
    filter_options.update({cls.__name__.replace("FilterOptions", "Task"): cls})
    return cls


@register_filter_options
class FactorialsFilterOptions(BaseModel):
    min_fact_val: float = Field(..., ge=0)


@register_filter_options
class ObservationWindowFilterOptions(BaseModel):
    min_window_duration_hours: float = Field(..., ge=0)
    max_window_delay_hours: float = Field(..., ge=0)
    window_selection: str


@register_filter_options
class ParameterFilterOptions(BaseModel):
    parameter_name: str
    parameter_requirement: Any
    parameter_comparison: str


# -------------- Output structs -----------------


class FactorialsTaskResult(BaseModel):
    factorial_result: float


class ObservationWindowTaskResult(BaseModel):
    windows: List[ObservationWindow]


class ParameterResult(BaseModel):
    parameter_name: str
    parameter_ok: bool
