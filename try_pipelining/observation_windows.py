"""
observation window calculation module
currently mostly being used by the CalculateObservability Task in tasks.py
"""

from datetime import date, timezone, datetime

import ephem
import numpy as np
from astropy import units as u
from astropy.coordinates import AltAz, Angle, SkyCoord, get_sun
from astropy.time import Time
from matplotlib.dates import date2num, num2date

from typing import List
from pydantic import BaseModel
from pydantic.errors import ConfigError


class ObservationWindow(BaseModel):
    start_time: datetime
    end_time: datetime
    delay_hours: float
    duration_hours: float


class Night(BaseModel):
    evening_date: date
    sun_set: datetime
    sun_rise: datetime


def setup_nights(alert, options, site) -> List[Night]:
    """Identifies the nights that should be probed for valid observation windows.

    Args:
        alert_received_time (datetime): [description]
        search_range (Quantity): [description]
        site (EarthLocation): [description]

    Returns:
        List[Night]: list of nights.
    """
    nights = []
    # identify how many nights to consider for observations window search
    min_time = Time(alert.alert_time, scale="utc")
    max_time = min_time + options.search_range_hours * u.hour

    end = min_time
    while end < max_time:
        if end > max_time:
            break

        sunset, sunrise = find_next_sun_rise_and_set(site, end.datetime)

        if sunset > max_time:
            break

        evening_date = date(sunset.year, sunset.month, sunset.day)
        nights.append(
            Night(evening_date=evening_date, sun_set=sunset, sun_rise=sunrise)
        )
        end = Time(sunrise)

    return nights


def setup_night_timerange(night, options):
    night_duration = night.sun_rise - night.sun_set
    n_steps = abs(
        int(night_duration.total_seconds() / (options.precision_minutes * 60))
    )

    time_range = np.linspace(date2num(night.sun_set), date2num(night.sun_rise), n_steps)
    date_range = [num2date(t) for t in time_range]
    return date_range


def apply_criteria_to_night(science_alert, options, site, night_test_dates):
    position = SkyCoord(
        science_alert.coords.raInDeg, science_alert.coords.decInDeg, unit="deg"
    )

    night_times = Time(night_test_dates)

    altaz_frame = AltAz(obstime=night_times, location=site.location)
    sun_alt_azs = get_sun(night_times).transform_to(altaz_frame)
    source_alt_az = position.transform_to(altaz_frame)

    moon = ephem.Moon()
    obs = ephem.Observer()
    obs.lon = str(site.lon / u.deg)
    obs.lat = str(site.lat / u.deg)
    obs.elev = site.height / u.m

    moon_alts = np.zeros_like(night_test_dates)
    moon_azs = np.zeros_like(night_test_dates)
    moon_phase = np.zeros_like(night_test_dates)

    for ii, tt in enumerate(night_times):
        obs.date = ephem.Date(tt.datetime)
        moon.compute(obs)
        moon_alts[ii] = moon.alt * 180.0 / np.pi
        moon_azs[ii] = moon.az * 180.0 / np.pi
        moon_phase[ii] = moon.phase

    moon_alt_az = AltAz(
        alt=Angle(moon_alts, unit=u.deg),
        az=Angle(moon_azs, unit=u.deg),
        location=site.location,
        obstime=night_times,
    )

    sun_alts = sun_alt_azs.alt / u.deg
    source_alts = source_alt_az.alt / u.deg
    moon_alts = moon_alt_az.alt / u.deg

    max_moon_alt = options.max_moon_altitude_deg
    max_sun_alt = options.max_sun_altitude_deg
    source_alt_limit = 90.0 - options.max_zenith_deg

    sun_mask = sun_alts < max_sun_alt
    source_mask = source_alts > source_alt_limit
    moon_alt_mask = moon_alts < max_moon_alt
    filter_mask = sun_mask & source_mask & moon_alt_mask

    night_date_nums = np.array([date2num(t.datetime) for t in night_times])
    valid_dates = night_date_nums[filter_mask]
    good_obs_times = [num2date(d) for d in valid_dates]

    return good_obs_times


def find_next_sun_rise_and_set(site, test_time):
    """calculates the next set and rise time of the sun with respect
    to the alert_received_time contained in the science alert"""
    sun = ephem.Sun()
    obs = ephem.Observer()
    obs.lon = str(site.lon / u.deg)
    obs.lat = str(site.lat / u.deg)
    obs.elev = site.height / u.m
    obs.date = test_time
    sun.compute(obs)

    set_time_local = obs.next_setting(sun, use_center=True).datetime()
    rise_time_local = obs.next_rising(sun, use_center=True).datetime()

    if set_time_local > rise_time_local:
        # if the test time is during the night, use the previous sun-set instead of the next one.
        set_time_local = obs.previous_setting(sun, use_center=True).datetime()

    # TODO: Make sure that UTC time is returned here.

    return set_time_local.replace(tzinfo=timezone.utc), rise_time_local.replace(
        tzinfo=timezone.utc
    )


def calculate_observation_windows(
    science_alert, options, site, testable_dates_nightlist
) -> List[ObservationWindow]:
    windows = []
    for testable_dates in testable_dates_nightlist:
        good_times = apply_criteria_to_night(
            science_alert, options, site, testable_dates
        )

        times_after_alert = [t for t in good_times if t > science_alert.alert_time]

        if not len(times_after_alert):
            continue

        start = times_after_alert[0]
        end = times_after_alert[-1]
        delay_in_hours = round(
            (start - science_alert.alert_time).total_seconds() / 60.0 / 60.0, 3
        )
        duration_in_hours = round((end - start).total_seconds() / 60.0 / 60.0, 3)
        windows.append(
            ObservationWindow(
                start_time=start,
                end_time=end,
                delay_hours=delay_in_hours,
                duration_hours=duration_in_hours,
            )
        )

    return windows


def select_observation_window(
    observation_windows: List[ObservationWindow], selection: str
) -> ObservationWindow:
    if selection not in selection_map.keys():
        raise KeyError(
            f"{selection} is not a valid selection method for ObservationWindows."
        )

    return selection_map[selection](observation_windows)


def select_earliest_window(windows: List[ObservationWindow]) -> ObservationWindow:
    return min([win for win in windows], key=lambda w: w.delay_hours)


def select_longest_window(windows: List[ObservationWindow]) -> ObservationWindow:
    return max([win for win in windows], key=lambda w: w.duration_hours)


selection_map = {"earliest": select_earliest_window, "longest": select_longest_window}
