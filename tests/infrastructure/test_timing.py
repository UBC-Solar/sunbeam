from types import SimpleNamespace

from pipeline.timing import StageTimingStats, TimingStats


def pipeline(name, frequency, stage_names):
    return SimpleNamespace(name=name, frequency=frequency, stage_names=stage_names, timing=StageTimingStats())


def run(stats, p, elapsed_ms, stage_ms):
    for stage, ms in stage_ms.items():
        p.timing.record(stage, int(ms * 1e6))
    stats.on_pipeline_start(p.name, 0)
    stats.on_pipeline_done(p.name, int(elapsed_ms * 1e6))


class TestReportOrder:
    def test_pipelines_and_stages_keep_their_order_whatever_they_took(self):
        fast = pipeline("10 Hz", 10.0, ["A", "B"])
        slow = pipeline("1 Hz", 1.0, ["C"])
        stats = TimingStats({fast.name: fast, slow.name: slow})

        # The second pipeline and the second stage take far longer
        run(stats, slow, 50.0, {"C": 50.0})
        run(stats, fast, 1.0, {"B": 0.9, "A": 0.1})
        payload = stats.metrics_payload_and_reset()

        assert [p["name"] for p in payload["pipelines"]] == ["10 Hz", "1 Hz"]
        assert [s["name"] for s in payload["pipelines"][0]["stages"]] == ["A", "B"]
        assert payload["pipelines"][0]["frequency"] == 10.0

    def test_a_pipeline_that_has_run_stays_listed_through_a_quiet_report(self):
        fast = pipeline("10 Hz", 10.0, ["A"])
        slow = pipeline("1 Hz", 1.0, ["C"])
        stats = TimingStats({fast.name: fast, slow.name: slow})

        run(stats, slow, 5.0, {"C": 5.0})
        stats.metrics_payload_and_reset()
        run(stats, fast, 1.0, {"A": 1.0})
        payload = stats.metrics_payload_and_reset()

        assert [(p["name"], p["ticks"]) for p in payload["pipelines"]] == [("10 Hz", 1), ("1 Hz", 0)]

    def test_a_pipeline_that_never_ran_isnt_listed(self):
        ingress = pipeline("Ingress", 0.0, ["Offline_Ingress"])
        fast = pipeline("10 Hz", 10.0, ["A"])
        stats = TimingStats({fast.name: fast, ingress.name: ingress})

        run(stats, fast, 1.0, {"A": 1.0})

        assert [p["name"] for p in stats.metrics_payload_and_reset()["pipelines"]] == ["10 Hz"]

    def test_reports_the_slowest_tick(self):
        fast = pipeline("10 Hz", 10.0, ["A"])
        stats = TimingStats({fast.name: fast})

        for ms in (1.0, 4.0, 2.0):
            run(stats, fast, ms, {"A": ms})
        metrics = stats.metrics_payload_and_reset()["pipelines"][0]

        assert (metrics["avg_ms"], metrics["max_ms"]) == (7.0 / 3, 4.0)
