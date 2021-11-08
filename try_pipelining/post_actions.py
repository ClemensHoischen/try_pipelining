from typing import List, Any, Union
from pydantic import BaseModel, Field, root_validator

from try_pipelining.data_models import ScienceAlert, SchedulingBlock
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


class PostAction:
    def __init__(self, science_alert, action_type, action_options):
        self.science_alert: ScienceAlert = science_alert
        self.action_type: str = action_type
        self.action_options: Union[
            CreateWobbleSchedulingBlockOptions, Any
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