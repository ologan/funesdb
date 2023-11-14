from rptest.services.funes import FunesService


class PartitionMetrics:
    def __init__(self, funes: FunesService):
        self.funes = funes

    """ Convenience class for grabbing partition metrics. """

    def bytes_produced(self):
        return self._sum_metrics(
            "vectorized_cluster_partition_bytes_produced_total")

    def records_produced(self):
        return self._sum_metrics(
            "vectorized_cluster_partition_records_produced")

    def bytes_fetched(self):
        return self._sum_metrics(
            "vectorized_cluster_partition_bytes_fetched_total")

    def records_fetched(self):
        return self._sum_metrics(
            "vectorized_cluster_partition_records_fetched")

    def _sum_metrics(self, metric_name):

        family = self.funes.metrics_sample(metric_name,
                                              nodes=self.funes.nodes)
        assert family, "Missing metrics."
        total = 0
        for sample in family.samples:
            total += sample.value

        self.funes.logger.debug(f"sum: {metric_name} - {total}")
        return total
