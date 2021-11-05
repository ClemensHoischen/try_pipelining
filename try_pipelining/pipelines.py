from try_pipelining.factorials import factorial
from try_pipelining import parameter
from typing import List

from try_pipelining.observation_windows import (
    setup_nights,
    setup_night_timerange,
    calculate_observation_windows,
)
from try_pipelining.data_models import (
    FactorialsFilterOptions,
    ObservationWindowTaskResult,
    FactorialsTaskResult,
    ObservationWindowFilterOptions,
    ParameterFilterOptions,
    ParameterResult,
)


class Pipeline:
    def __init__(self, science_alert, site, options):
        self.science_alert = science_alert
        self.site = site
        self.options = options


class FactorialPipeline(Pipeline):
    def run(self):
        return FactorialsTaskResult(factorial_result=factorial(self.options.fact_n))

    def filter(
        self, result: FactorialsTaskResult, filter_options: FactorialsFilterOptions
    ) -> List[FactorialsTaskResult]:
        assert isinstance(result, FactorialsTaskResult)
        assert isinstance(filter_options, FactorialsFilterOptions)

        if result.factorial_result > filter_options.min_fact_val:
            return [result]

        return []


class ObservationWindowPipeline(Pipeline):
    def run(self):
        nights = setup_nights(self.science_alert, self.options, self.site)
        testable_dates_nightlist = [
            setup_night_timerange(night, self.options) for night in nights
        ]
        observation_windows = calculate_observation_windows(
            self.science_alert, self.options, self.site, testable_dates_nightlist
        )
        return ObservationWindowTaskResult(windows=observation_windows)

    def filter(
        self,
        result: ObservationWindowTaskResult,
        filter_options: ObservationWindowFilterOptions,
    ) -> List[ObservationWindowTaskResult]:
        assert isinstance(result, ObservationWindowTaskResult)
        assert isinstance(filter_options, ObservationWindowFilterOptions)

        filtered_window_results = []
        for window in result.windows:
            delay_ok = window.delay_hours < filter_options.max_window_delay_hours
            duration_ok = (
                window.duration_hours > filter_options.min_window_duration_hours
            )

            if delay_ok and duration_ok:
                filtered_window_results.append(
                    ObservationWindowTaskResult(windows=[window])
                )

        return filtered_window_results


class ParameterPipeline(Pipeline):
    def run(self):
        return None

    def filter(
        self, result: None, filter_options: ParameterFilterOptions
    ) -> ParameterResult:
        pars: dict = self.science_alert.measured_parameters
        return ParameterResult(
            parameter_name=filter_options.parameter_name,
            parameter_ok=parameter.execute_parameter_filtering(pars, filter_options),
        )


pipeline_map = {
    "FactorialsPipeline": FactorialPipeline,
    "ObservationWindowPipeline": ObservationWindowPipeline,
    "ParameterPipeline": ParameterPipeline,
}
