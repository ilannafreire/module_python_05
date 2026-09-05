from abc import ABC, abstractmethod
from typing import Any, Protocol


class DataProcessor(ABC):
    """Abstract base class defining the common processing interface."""

    def __init__(self) -> None:
        self.data: list[tuple[int, str]] = []
        self.rank: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Return whether data can be processed."""
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        """Process and store data."""
        pass

    def output(self) -> tuple[int, str]:
        """Remove and return the oldest processed item."""
        if not self.data:
            raise IndexError("No data available")
        return self.data.pop(0)

    def _store(self, value: str) -> None:
        """Store a value with its processing rank."""
        self.data.append((self.rank, value))
        self.rank += 1


class NumericProcessor(DataProcessor):
    """Process numeric values and lists of numeric values."""

    def validate(self, data: Any) -> bool:
        values = data if isinstance(data, list) else [data]
        return all(
            isinstance(value, (int, float)) and not isinstance(value, bool)
            for value in values
        )

    def ingest(self, data: int | float | list[int | float]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        values = data if isinstance(data, list) else [data]
        for value in values:
            self._store(str(value))


class TextProcessor(DataProcessor):
    """Process text values and lists of text values."""

    def validate(self, data: Any) -> bool:
        values = data if isinstance(data, list) else [data]
        return all(isinstance(value, str) for value in values)

    def ingest(self, data: str | list[str]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")
        values = data if isinstance(data, list) else [data]
        for value in values:
            self._store(value)


class LogProcessor(DataProcessor):
    """Process log entries and lists of log entries."""

    def validate(self, data: Any) -> bool:
        entries = data if isinstance(data, list) else [data]
        return all(
            isinstance(entry, dict)
            and isinstance(entry.get("log_level"), str)
            and isinstance(entry.get("log_message"), str)
            and all(
                isinstance(key, str) and isinstance(value, str)
                for key, value in entry.items()
            )
            for entry in entries
        )

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")
        entries = data if isinstance(data, list) else [data]
        for entry in entries:
            self._store(f"{entry['log_level']}: {entry['log_message']}")


class ExportPlugin(Protocol):
    """Define the output interface expected by DataStream."""

    def process_output(self, data: list[tuple[int, str]]) -> None:
        """Export processed items."""
        ...


class CSVExportPlugin:
    """Export processed values as a single CSV row."""

    @staticmethod
    def _escape(value: str) -> str:
        """Escape a CSV field only when quoting is necessary."""
        if any(character in value for character in ',"\n\r'):
            return '"' + value.replace('"', '""') + '"'
        return value

    def process_output(self, data: list[tuple[int, str]]) -> None:
        """Print the values from a processor in CSV format."""
        values = ",".join(self._escape(value) for _, value in data)
        print("CSV Output:")
        print(values)


class JSONExportPlugin:
    """Export processed values as a JSON object."""

    @staticmethod
    def _escape(value: str) -> str:
        """Escape the characters required in a JSON string."""
        return (
            value.replace("\\", "\\\\")
            .replace('"', '\\"')
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t")
        )

    def process_output(self, data: list[tuple[int, str]]) -> None:
        """Print the values from a processor in JSON format."""
        items = [
            f'"item_{rank}": "{self._escape(value)}"'
            for rank, value in data
        ]
        print("JSON Output:")
        print("{" + ", ".join(items) + "}")


class DataStream:
    """Route stream elements and export processed output."""

    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        """Register a processor for incoming stream elements."""
        self.processors.append(proc)

    def _find_processor(self, element: Any) -> DataProcessor | None:
        """Return the first processor that accepts element."""
        return next(
            (proc for proc in self.processors if proc.validate(element)),
            None,
        )

    def process_stream(self, stream: list[Any]) -> None:
        """Send every stream element to a compatible processor."""
        for element in stream:
            processor = self._find_processor(element)
            if processor is None:
                print(f"DataStream error - Can't process element "
                      f"in stream: {element}")
            else:
                processor.ingest(element)

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        """Export up to nb oldest items from each registered processor."""
        if nb < 0:
            raise ValueError("Number of output items cannot be negative")

        for processor in self.processors:
            output_data: list[tuple[int, str]] = []
            for _ in range(min(nb, len(processor.data))):
                output_data.append(processor.output())
            if output_data:
                plugin.process_output(output_data)

    def print_processors_stats(self) -> None:
        """Print totals and remaining items for every processor."""
        print("== DataStream statistics ==")
        if not self.processors:
            print("No processor found, no data")
            return
        for processor in self.processors:
            name = processor.__class__.__name__.removesuffix("Processor")
            print(
                f"{name} Processor: total {processor.rank} items processed, "
                f"remaining {len(processor.data)} on processor"
            )


def main() -> None:
    """Demonstrate the full data-processing and export pipeline."""
    print("=== Code Nexus - Data Pipeline ===")

    stream = DataStream()
    print("\nInitialize Data Stream...\n")
    stream.print_processors_stats()

    print("\nRegistering Processors")
    stream.register_processor(NumericProcessor())
    stream.register_processor(TextProcessor())
    stream.register_processor(LogProcessor())

    first_batch: list[Any] = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {
                "log_level": "WARNING",
                "log_message": "Telnet access! Use ssh instead",
            },
            {"log_level": "INFO", "log_message": "User wil is connected"},
        ],
        42,
        ["Hi", "five"],
    ]
    print(f"\nSend first batch of data on stream: {first_batch}")
    stream.process_stream(first_batch)
    print()
    stream.print_processors_stats()

    print("\nSend 3 processed data from each processor to a CSV plugin:")
    stream.output_pipeline(3, CSVExportPlugin())
    print()
    stream.print_processors_stats()

    second_batch: list[Any] = [
        21,
        ["I love AI", "LLMs are wonderful", "Stay healthy"],
        [
            {"log_level": "ERROR", "log_message": "500 server crash"},
            {
                "log_level": "NOTICE",
                "log_message": "Certificate expires in 10 days",
            },
        ],
        [32, 42, 64, 84, 128, 168],
        "World hello",
    ]
    print(f"\nSend another batch of data: {second_batch}")
    stream.process_stream(second_batch)
    print()
    stream.print_processors_stats()

    print("\nSend 5 processed data from each processor to a JSON plugin:")
    stream.output_pipeline(5, JSONExportPlugin())
    print()
    stream.print_processors_stats()


if __name__ == "__main__":
    main()
