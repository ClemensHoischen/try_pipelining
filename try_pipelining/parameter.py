# Helper class to work with the paramters

from typing import Any

from try_pipelining.data_models import ParameterOptions

evaluators = {}


def register_evaluator(func):
    evaluators.update({func.__name__: func})
    return func


def execute_parameter_filtering(
    parameters: dict, parameter_filtering_options: ParameterOptions
):
    parameter_to_filter: str = parameter_filtering_options.parameter_name
    required_value: Any = parameter_filtering_options.parameter_requirement
    comparison_mode: str = parameter_filtering_options.parameter_comparison

    actual_parameter = parameters.get(parameter_to_filter)
    print(comparison_mode)
    if comparison_mode == "equal":
        print(actual_parameter, required_value)
        print(evaluators[comparison_mode])

    return evaluators[comparison_mode](actual_parameter, required_value)


@register_evaluator
def greater(par, req):
    if par > req:
        return True

    return False


@register_evaluator
def less(par, req):
    if par < req:
        return True

    return False


@register_evaluator
def equal(par, req):
    if par == req:
        return True

    return False