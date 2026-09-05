from typing import Any

from ex0.data_processor import (  # type: ignore
    LogProcessor as Ex0LogProcessor,
    NumericProcessor as Ex0NumericProcessor,
    TextProcessor as Ex0TextProcessor,
)
from ex1.data_stream import (  # type: ignore
    DataStream as Ex1DataStream,
    LogProcessor as Ex1LogProcessor,
    NumericProcessor as Ex1NumericProcessor,
    TextProcessor as Ex1TextProcessor,
)
from ex2.data_pipeline import (  # type: ignore
    DataStream as Ex2DataStream,
    LogProcessor as Ex2LogProcessor,
    NumericProcessor as Ex2NumericProcessor,
    TextProcessor as Ex2TextProcessor,
)


class CapturePlugin:
    """Store exported data so Exercise 2 can be verified."""

    def __init__(self) -> None:
        self.batches: list[list[tuple[int, str]]] = []

    def process_output(self, data: list[tuple[int, str]]) -> None:
        """Store an exported batch."""
        self.batches.append(data)


def test_exercise_0() -> None:
    """Test validation, ingestion, ranks, and invalid input handling."""
    numeric = Ex0NumericProcessor()
    text = Ex0TextProcessor()
    log = Ex0LogProcessor()

    assert numeric.validate([1, 2.5])
    assert not numeric.validate("not a number")
    assert text.validate(["Code", "Nexus"])
    assert not text.validate(42)
    assert log.validate({"log_level": "INFO", "log_message": "Ready"})
    assert not log.validate({"log_level": "INFO", "log_message": 42})

    numeric.ingest([1, 2.5])
    assert numeric.output() == (0, "1")
    assert numeric.output() == (1, "2.5")

    invalid_data: Any = "not a number"
    try:
        numeric.ingest(invalid_data)
    except ValueError:
        pass
    else:
        raise AssertionError("NumericProcessor accepted invalid data")


def test_exercise_1() -> None:
    """Test polymorphic routing and an element without a processor."""
    stream = Ex1DataStream()
    numeric = Ex1NumericProcessor()
    text = Ex1TextProcessor()
    log = Ex1LogProcessor()
    stream.register_processor(numeric)

    stream.process_stream(["No text processor is registered yet"])
    assert numeric.data == []

    stream.register_processor(text)
    stream.register_processor(log)

    stream.process_stream([
        [3.14, -1],
        "Hello",
        {"log_level": "WARNING", "log_message": "Check connection"},
    ])

    assert numeric.data == [(0, "3.14"), (1, "-1")]
    assert text.data == [(0, "Hello")]
    assert log.data == [(0, "WARNING: Check connection")]


def test_exercise_2() -> None:
    """Test output_pipeline, including invalid output amounts."""
    stream = Ex2DataStream()
    stream.register_processor(Ex2NumericProcessor())
    stream.register_processor(Ex2TextProcessor())
    stream.register_processor(Ex2LogProcessor())

    plugin = CapturePlugin()
    try:
        stream.output_pipeline(-1, plugin)
    except ValueError:
        pass
    else:
        raise AssertionError("output_pipeline accepted a negative amount")

    stream.process_stream([
        [1, 2],
        "Nexus",
        {"log_level": "INFO", "log_message": "Pipeline ready"},
    ])
    stream.output_pipeline(2, plugin)

    assert plugin.batches == [
        [(0, "1"), (1, "2")],
        [(0, "Nexus")],
        [(0, "INFO: Pipeline ready")],
    ]


def main() -> None:
    """Run the local verification for all exercises."""
    tests = [
        ("Exercise 0", test_exercise_0),
        ("Exercise 1", test_exercise_1),
        ("Exercise 2", test_exercise_2),
    ]

    print("=== Code Nexus - Local Test Suite ===")
    for name, test in tests:
        test()
        print(f"{name}: passed")
    print("All exercises passed.")


if __name__ == "__main__":
    main()
