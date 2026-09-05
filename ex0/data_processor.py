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


if __name__ == "__main__":
    numeric = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()

    print("=== Code Nexus - Data Processor ===")

    print("\nTesting Numeric Processor...")
    print(f"42: {numeric.validate(42)}")
    print(f"Hello: {numeric.validate('Hello')}")
    try:
        numeric.ingest("foo")  # type: ignore[arg-type]
    except ValueError as error:
        print(f"Got exception: {error}")
    numeric.ingest([1, 2, 3, 4, 5])
    print("Extracting 3 values...")
    for _ in range(3):
        rank, value = numeric.output()
        print(f"Numeric value {rank}: {value}")

    print("\nTesting Text Processor...")
    print(f"Hello: {text.validate('Hello')}")
    print(f"42: {text.validate(42)}")
    text.ingest(["Hello", "Nexus", "World"])
    rank, value = text.output()
    print(f"Text value {rank}: {value}")

    print("\nTesting Log Processor...")
    print(
        "Valid log: "
        f"{log.validate({'log_level': 'INFO', 'log_message': 'Connected'})}"
    )
    print(f"Hello: {log.validate('Hello')}")
    log.ingest([
        {"log_level": "NOTICE", "log_message": "Connection to server"},
        {"log_level": "ERROR", "log_message": "Unauthorized access!!"},
    ])
    for _ in range(2):
        rank, value = log.output()
        print(f"Log entry {rank}: {value}")
