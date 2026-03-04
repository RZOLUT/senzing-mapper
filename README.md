# senzing-mapper

## Overview

The Rzolut mapper converts Rzolut compliance/risk dataset records (JSONL) into Senzing-compatible JSON for entity resolution. It handles PEP, sanctions, watchlists, enforcement, and adverse press data.

## Repository Contents

```
src/
  rzolut_mapper.py      # The mapper
  rzolut_codes.csv      # Identifier type classification (338 types)
sample/
  input.jsonl           # Example Rzolut source records
  input_pretty.json     # Pretty-printed example input
  output.json           # Example mapped Senzing JSON output
  output_pretty.json    # Pretty-printed example output
```

## Usage

```bash
python3 src/rzolut_mapper.py -i <input_file> -o <output_file> -d <data_source_code> [-l <log_file>]
```

**Arguments:**
- `-i, --input_file` -- Path to the Rzolut JSONL input file
- `-o, --output_file` -- Path for the mapped Senzing JSON output
- `-d, --data_source` -- Data source code (e.g., `RZOLUT`)
- `-l, --log_file` -- (Optional) Path to write processing statistics as JSON

**Example:**
```bash
python3 src/rzolut_mapper.py -i data/rzolut_full.jsonl -o output/rzolut.json -d RZOLUT -l output/stats.json
```

## Prerequisites

- Python 3.10+
- No external dependencies (stdlib only)

## Identifier Codes (`src/rzolut_codes.csv`)

The mapper uses `rzolut_codes.csv` to classify identifier types. Each row maps a Rzolut identifier name to a Senzing feature type.

**CSV columns:**
| Column | Description |
|--------|-------------|
| `num` | Sequential row number |
| `code_type` | Always "Identifier" |
| `code` | The identifier name as it appears in the Rzolut data |
| `country` | Country where this identifier is used |
| `subject_type` | Individual, Organization, etc. |
| `senzing_feature` | Senzing feature type (NATIONAL_ID, TAX_ID, PASSPORT, etc.) |
| `senzing_type_value` | Sub-type value (AADHAAR, CPF, CIN, etc.) |
| `disposition` | How the mapper handles this type (see below) |
| `notes` | Description of the identifier |

**Dispositions:**
- **FEATURE** -- Mapped to a Senzing feature for entity resolution (e.g., PASSPORT, NATIONAL_ID, TAX_ID)
- **PAYLOAD** -- Stored as record payload for human review, not used in matching (e.g., arrest warrant numbers, certificate numbers)
- **MISSING** -- Auto-added by the mapper when an unknown identifier type is encountered; needs manual classification

### Handling MISSING Codes

If the mapper encounters identifier types not in the CSV, it:

1. Stores the value as record payload (safe default -- no impact on entity resolution)
2. Appends the new type to `rzolut_codes.csv` with disposition `MISSING`
3. Prints a message at the end: `N new identifier codes added to rzolut_codes.csv (disposition: MISSING)`

**What to do:** Open `rzolut_codes.csv`, find the rows with disposition `MISSING`, and classify them:
- Set `senzing_feature` and `senzing_type_value` and change disposition to `FEATURE` if the identifier is useful for entity resolution
- Change disposition to `PAYLOAD` if it is not useful for matching

## Statistics Log File

When you use the `-l` flag, the mapper writes a JSON file with processing statistics including:
- **`!IDTYPE`** -- Counts and examples of every identifier type encountered
- **`!INFO` / `BAD_DATE`** -- Any dates that could not be parsed

This is useful for reviewing the distribution of identifier types in your data and confirming that the codes CSV covers your dataset.
