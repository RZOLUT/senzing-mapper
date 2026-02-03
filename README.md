# senzing-mapper

## Overview
The rzolut_mapper.py is a Python-based data mapping utility that converts the Rzolut dataset into Senzing-compatible JSON.
The mapper standardises, cleans, and validates complex attributes ensuring high-quality input for downstream entity resolution and compliance systems.

Usage :
The mapper is executed from the command line and requires an input file, output file, and data source code.
```console
python3 rzolut_mapper.py -i <input_file> -o <output_file> -d <data_source_code>

command line arguments:
• -i, --input_file: The path to the raw data file (in JSON Lines format).
• -o, --output_file: The desired path for the processed JSON output.
• -d, --data_source: A required code identifying the source of the data.
• -l, --log_file: (Optional) Name of the file to store processing statistics
```
