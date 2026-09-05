from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    """Abstract base class defining the common processing interface."""

    def __init__(self) -> None:
        self.data: list[tuple[int, str]] = []
        self.rank: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Abstract method to validate if data fits this processor."""
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        """Abstract method to process and store data."""
        pass

    def output(self) -> tuple[int, str]:
        """Pop and return the oldest stored (rank, value) pair."""
        if not self.data:
            raise IndexError("No data available")
        return self.data.pop(0)

    def _store(self, value: str) -> None:
        """Store a processed value with its processing rank."""
        self.data.append((self.rank, value))
        self.rank += 1


class NumericProcessor(DataProcessor):
    """Processes int, float, and lists of numeric values."""

    def validate(self, data: Any) -> bool:
        """Check that data is a number or a list of numbers."""
        values = data if isinstance(data, list) else [data]
        return all(
            isinstance(value, (int, float)) and not isinstance(value, bool)
            for value in values
        )

    def ingest(self, data: int | float | list[int | float]) -> None:
        """Validate then store numeric data as strings."""
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        values = data if isinstance(data, list) else [data]
        for value in values:
            self._store(str(value))


class TextProcessor(DataProcessor):
    """Processes str and lists of strings."""

    def validate(self, data: Any) -> bool:
        """Check that data is a string or a list of strings."""
        values = data if isinstance(data, list) else [data]
        return all(isinstance(v, str) for v in values)

    def ingest(self, data: str | list[str]) -> None:
        """Validate then store text data."""
        if not self.validate(data):
            raise ValueError("Improper text data")
        values = data if isinstance(data, list) else [data]
        for value in values:
            self._store(value)


class LogProcessor(DataProcessor):
    """Processes dict log entries and lists of log entries."""

    def validate(self, data: Any) -> bool:
        """Check that data is a valid log entry or list of entries."""
        entries = data if isinstance(data, list) else [data]
        return all(
            isinstance(e, dict)
            and isinstance(e.get("log_level"), str)
            and isinstance(e.get("log_message"), str)
            and all(isinstance(key, str) and isinstance(value, str)
                    for key, value in e.items())
            for e in entries
        )

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        """Validate then store log entries as formatted strings."""
        if not self.validate(data):
            raise ValueError("Improper log data")
        entries = data if isinstance(data, list) else [data]
        for entry in entries:
            self._store(f"{entry['log_level']}: {entry['log_message']}")


class DataStream:
    """Routes stream elements to registered processors polymorphically."""

    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        """Register a new data processor to handle stream elements."""
        self.processors.append(proc)

    def _find_processor(self, element: Any) -> DataProcessor | None:
        """Return the first registered processor that accepts element."""
        return next(
            (proc for proc in self.processors if proc.validate(element)),
            None,
        )

    def process_stream(self, stream: list[Any]) -> None:
        """Route each element to the first processor that accepts it."""
        for element in stream:
            proc = self._find_processor(element)
            if proc is None:
                print(f"DataStream error - Can't process element "
                      f"in stream: {element}")
            else:
                proc.ingest(element)

    def print_processors_stats(self) -> None:
        """Print processing statistics for all registered processors."""
        print("== DataStream statistics ==")
        if not self.processors:
            print("No processor found, no data")
            return
        for proc in self.processors:
            name = proc.__class__.__name__.removesuffix("Processor")
            total = proc.rank
            remaining = len(proc.data)
            print(f"{name} Processor: total {total} items processed, "
                  f"remaining {remaining} on processor")


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===")

    print("\nInitialize Data Stream...")
    stream_manager = DataStream()
    stream_manager.print_processors_stats()

    print("\nRegistering Numeric Processor")
    numeric = NumericProcessor()
    stream_manager.register_processor(numeric)

    batch = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {"log_level": "WARNING",
             "log_message": "Telnet access! Use ssh instead"},
            {"log_level": "INFO", "log_message": "User wil is connected"},
        ],
        42,
        ["Hi", "five"],
    ]
    print(f"\nSend first batch of data on stream: {batch}")
    stream_manager.process_stream(batch)
    stream_manager.print_processors_stats()

    print("\nRegistering other data processors")
    text = TextProcessor()
    log = LogProcessor()
    stream_manager.register_processor(text)
    stream_manager.register_processor(log)

    print("Send the same batch again")
    stream_manager.process_stream(batch)
    stream_manager.print_processors_stats()

    print("\nConsume some elements from the data processors: "
          "Numeric 3, Text 2, Log 1")
    for _ in range(3):
        rank, value = numeric.output()
        print(f"Numeric value {rank}: {value}")
    for _ in range(2):
        rank, value = text.output()
        print(f"Text value {rank}: {value}")
    rank, value = log.output()
    print(f"Log entry {rank}: {value}")
    stream_manager.print_processors_stats()
