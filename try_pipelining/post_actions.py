from datetime import datetime, timedelta
from math import floor
from itertools import cycle

from typing import List, Any, Union
from pydantic import BaseModel, Field, root_validator

from astropy.coordinates import Angle, SkyCoord
from astropy import units as u

from try_pipelining.data_models import (
    ScienceAlert,
    SchedulingBlock,
    ObservationBlock,
    Coords,
    WobbleOptions,
)
from try_pipelining.observation_windows import ObservationWindow

available_post_actions = {}
available_post_action_options = {}


def register_post_action(cls):
    available_post_actions.update({cls.__name__: cls})
    return cls


def register_post_action_option(cls):
    available_post_action_options.update({cls.__name__.replace("Options", ""): cls})
    return cls


class Wobble(BaseModel):
    offsets: List[float]
    angles: List[float]

    @root_validator
    def validate_same_length(cls, values):
        if len(values.get("offsets")) != len(values.get("angles")):
            raise AttributeError("offsets and angles lists should be of same length.")

        return values


class Proposal(BaseModel):
    proposal_id: int
    proposal_class: str = Field(..., max_length=1)
    proposal_rank: float = Field(..., ge=0)


@register_post_action_option
class CreateWobbleSchedulingBlockOptions(BaseModel):
    wobble: Wobble
    proposal: Proposal


@register_post_action_option
class CreateObservationBlocksOptions(BaseModel):
    min_block_duration_minutes: float = Field(..., ge=0)
    max_block_duration_minutes: float = Field(..., ge=0)


class PostAction:
    def __init__(self, science_alert, action_type, action_options):
        self.science_alert: ScienceAlert = science_alert
        self.action_type: str = action_type
        self.action_options: Union[
            CreateWobbleSchedulingBlockOptions,
            CreateObservationBlocksOptions,
        ] = action_options
        self.validate()

    def validate(self):
        assert isinstance(
            self.action_options, available_post_action_options[self.action_type]
        )

    def run(self, task_result: Any):
        raise NotImplementedError(
            "A Raw Post Action class instance should not be used.\
            Use Chiled classes of PostAction only."
        )


@register_post_action
class CreateWobbleSchedulingBlock(PostAction):
    def run(self, task_result: ObservationWindow) -> SchedulingBlock:
        action_options: CreateWobbleSchedulingBlockOptions = self.action_options
        sb_dict = {
            "coords": self.science_alert.coords,
            "time_constraints": {
                "start_time": task_result.start_time,
                "end_time": task_result.end_time,
            },
            "wobble_options": action_options.wobble,
            "proposal": action_options.proposal,
        }
        return SchedulingBlock(**sb_dict)


@register_post_action
class CreateObservationBlocks(PostAction):
    def run(self, task_result: SchedulingBlock) -> List[ObservationBlock]:
        base_target_coords: Coords = task_result.coords
        sb_start: datetime = task_result.time_constraints.start_time
        sb_end: datetime = task_result.time_constraints.end_time
        sb_duration_sec = (sb_end - sb_start).total_seconds()
        wobble_opts: WobbleOptions = task_result.wobble_options

        action_options = self.action_options
        assert isinstance(action_options, CreateObservationBlocksOptions)

        n_blocks_max = floor(
            (sb_duration_sec / 60.0) / action_options.min_block_duration_minutes
        )

        #  TODO: get the optimal number of observation blocks (i.e. symetrical and not too short)
        #  going with max number of blocks for now...

        cycle_wobble_offset = cycle(wobble_opts.offsets)
        cycle_wobble_angles = cycle(wobble_opts.angles)
        base_target = SkyCoord(
            ra=base_target_coords.raInDeg * u.deg,
            dec=base_target_coords.decInDeg * u.deg,
            frame="fk5",
        )

        obs = []
        for i in range(n_blocks_max):
            coord_with_offset = base_target.directional_offset_by(
                position_angle=next(cycle_wobble_angles),
                separation=next(cycle_wobble_offset),
            )
            ra = coord_with_offset.ra / u.deg
            dec = coord_with_offset.dec / u.deg
            ra_in_deg = ra if ra > 0 else ra + 180
            dec_in_deg = dec if dec > 0 else dec + 180

            obs.append(
                ObservationBlock(
                    start_time=sb_start
                    + timedelta(minutes=i * action_options.min_block_duration_minutes),
                    end_time=sb_start
                    + timedelta(
                        minutes=(i + 1) * action_options.min_block_duration_minutes
                    ),
                    ra_target_deg=ra_in_deg,
                    dec_target_deg=dec_in_deg,
                )
            )

        return obs