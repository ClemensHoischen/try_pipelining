# Helper class to work with the paramters

from typing import Any
from try_pipelining.data_models import ParameterOptions


def execute_parameter_filtering(
    parameters: dict, parameter_filtering_options: ParameterOptions
):
    parameter_to_filter: str = parameter_filtering_options.parameter_name
    required_value: Any = parameter_filtering_options.parameter_requirement
    comparison_mode: str = parameter_filtering_options.parameter_comparison

    actual_parameter = parameters.get(parameter_to_filter)

    return evaluate_paramter_map[comparison_mode](actual_parameter, required_value)


def is_greater(par, req):
    if par > req:
        return True

    return False


def is_equal(par, req):
    if par == req:
        return True

    return False


evaluate_paramter_map = {"greater": is_greater, "equal": is_equal}
