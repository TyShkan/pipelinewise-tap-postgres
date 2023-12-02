import time
from singer import metrics


def record_counter(endpoint=None, metric_type="total", log_interval=metrics.DEFAULT_LOG_INTERVAL):
    tags = {}

    if endpoint:
        tags[metrics.Tag.endpoint] = endpoint

    metric = metrics.Metric.record_count

    if metric_type and metric_type != "total":
        metric += "_" + metric_type

    return metrics.Counter(metric, tags, log_interval=log_interval)


class CounterDynamic():
    def __init__(self, log_interval=metrics.DEFAULT_LOG_INTERVAL):
        self.log_interval = log_interval
        self.init_log_time = time.time()
        self.counters = {}

    def increment(self, endpoint, metric_type="total", amount=1):
        if endpoint not in self.counters:
            self.counters[endpoint] = {}

        if metric_type not in self.counters[endpoint]:
            self.counters[endpoint][metric_type] = record_counter(
                endpoint=endpoint,
                metric_type=metric_type,
                log_interval=self.log_interval
            )
            self.counters[endpoint][metric_type].last_log_time = self.init_log_time

        self.counters[endpoint][metric_type].increment(amount=amount)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        for endpoint in self.counters.values():
            for counter in endpoint.values():
                counter.__exit__(exc_type, exc_value, traceback)


def record_counter_dynamic(log_interval=metrics.DEFAULT_LOG_INTERVAL):
    return CounterDynamic(log_interval=log_interval)
