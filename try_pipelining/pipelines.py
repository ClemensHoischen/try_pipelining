from try_pipelining.factorials import factorial

from try_pipelining.observation_windows import (
    setup_nights,
    setup_night_timerange,
    calculate_observation_windows,
)


class Pipeline:
    def __init__(self, science_alert, site, options):
        self.science_alert = science_alert
        self.site = site
        self.options = options


class FactorialPipeline(Pipeline):
    def run(self):
        return factorial(self.options.fact_n)


class ObservationWindowPipeline(Pipeline):
    def run(self):
        nights = setup_nights(self.science_alert, self.options, self.site)
        testable_dates_nightlist = [
            setup_night_timerange(night, self.options) for night in nights
        ]
        observation_windows = calculate_observation_windows(
            self.science_alert, self.options, self.site, testable_dates_nightlist
        )
        return observation_windows


pipeline_map = {
    "FactorialsPipeline": FactorialPipeline,
    "ObservationWindowPipeline": ObservationWindowPipeline,
}
