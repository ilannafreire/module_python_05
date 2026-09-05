# Python Module 05 — Code Nexus: Polymorphic Data Streams

This project is part of the 42 School Common Core and explores abstraction,
inheritance, method overriding, and polymorphism in Python.

In the Code Nexus universe, different data types travel through the same
processing flow. Each type has its own validation and transformation rules,
while all processors share a common interface. The project demonstrates how to
create an extensible system without coupling the data flow to specific
implementations.

The main goals are to:

- create abstract classes with `ABC`;
- override methods to specialize behavior;
- process heterogeneous data through polymorphism;
- use `Protocol` to define structural export contracts;
- apply type annotations throughout the code;
- maintain a simple, extensible, and safe architecture.

---

## Project Structure

Each exercise is placed in its own directory and contains only the file
requested by the subject.

```text
ex0/
└── data_processor.py
ex1/
└── data_stream.py
ex2/
└── data_pipeline.py
main.py
```

`main.py` is a local test runner for all exercises and is not part of the
subject submission files.

---

## Exercises

### Exercise 0 — Data Processor

Implements the foundation of the processing system.

- Defines the abstract `DataProcessor` class.
- Creates `NumericProcessor`, `TextProcessor`, and `LogProcessor`.
- Each processor validates and ingests only data compatible with its type.
- Processed data is stored with a sequential rank.
- The `output()` method removes and returns the oldest item, following a FIFO
  model.

Concepts: `ABC`, abstract methods, inheritance, method overriding, and data
validation.

### Exercise 1 — Polymorphic Processing of a Data Stream

Adds the `DataStream` class, which routes stream elements to the appropriate
processor.

- Registers processors with `register_processor()`.
- Analyzes each element with `process_stream()`.
- Uses `validate()` to dynamically select a compatible processor.
- Reports elements that cannot be handled by any registered processor.
- Displays processed totals and remaining items for every processor.

Concepts: subtype polymorphism, shared interfaces, and heterogeneous data
routing.

### Exercise 2 — Data Pipeline

Completes the flow by adding an export step decoupled from the processors.

- Defines `ExportPlugin` with `Protocol`.
- Adds `DataStream.output_pipeline()` to consume data from registered
  processors.
- Implements `CSVExportPlugin` and `JSONExportPlugin`.
- Builds CSV and JSON strings manually, without dedicated libraries.
- Demonstrates that any object with a compatible `process_output()` method can
  act as an export plugin.

Concepts: duck typing, `Protocol`, structural polymorphism, and extensibility.

---

## Running the Project

Use Python 3.10 or later.

```bash
python3 ex0/data_processor.py
python3 ex1/data_stream.py
python3 ex2/data_pipeline.py
python3 main.py
```

---

## Quality Checks

The code uses only the imports authorized by the subject (`abc` and `typing`),
includes type annotations, and can be checked with:

```bash
flake8 ex0/data_processor.py ex1/data_stream.py ex2/data_pipeline.py
mypy ex0/data_processor.py ex1/data_stream.py ex2/data_pipeline.py
```

---

## Architecture

The project was built to allow extensions with minimal changes:

- A new data type can be handled by creating a new `DataProcessor` subclass.
- `DataStream` does not need to know each processor's internal implementation;
  it only uses their shared interface.
- A new output format can be added by creating a class compatible with
  `ExportPlugin`, without modifying `DataStream`.

This separation between processing, routing, and exporting is the central idea
of Code Nexus: different components cooperate through clear contracts instead
of rigid dependencies.
