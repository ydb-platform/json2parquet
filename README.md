# JSON to Parquet Converter (C++)

## Description

This C++ program is a JSON to Parquet data converter designed to read JSON input from stdin and write Parquet output to stdout. It is built using CMake and requires Apache Arrow version 11 or higher for compilation. The tool efficiently transforms data from JSON format to Parquet while maintaining the data structure.

## Usage

### Prerequisites

- CMake
- Apache Arrow (version 11 or higher)

### Build Instructions

```bash
mkdir build
cd build
cmake ..
make
```

### Command Line Usage

```bash
cat input.json | ./json2parquet --schema-file schema.txt > output.parquet
```

## Options

1. **Schema File:** Provide a schema file (`schema.txt`) to specify column types. The schema file should list column names and their corresponding data types. Supported types include date, int32, and int64. If a column from the JSON is not present in the schema file, the type will be determined automatically.

    **Example schema.txt:**
    ```plaintext
    column1 int32
    column2 date
    column3 int64
    ```

    ```bash
    cat input.json | ./json2parquet --schema-file schema.txt > output.parquet
    ```

2. **Optimizations:** Utilize options for optimizing the conversion process.

    ```bash
    cat input.json | ./json2parquet --compression snappy --max-row-group-length 100000 > output.parquet
    ```

## Dependencies

- CMake
- Apache Arrow (version 11 or higher)

## Contributions

If you have suggestions for improvement or encounter issues, feel free to create an issue or submit a pull request.
