# Introduction
This library adds some barebones functionality to remove a lot of boilerplate code from ETL/ELT pipelines for the fao_dwh_review project

## How to use it
- Define your workspace
- Create a Schema object from your dataframe
- Use the set_dimensions() and set_measures() methods. You can either pass strings or use Dimension/Measure objects for more control.
- Upload the jsonstats to the DWH with workspace's to_dwh() method

```
import fao_pypd_dwh as dwh
import pandas as pd
import datetime

df = pd.DataFrame(
    {
        "country_id": [1, 1, 2, 3],
        "country_name": ["Italy", "Italy", "Egypt", "France"],
        "another_column": ["dog", "cat", "pigeon", "opossum"],
        "date_column": [
            datetime.date(2024, 1, 1),
            datetime.date(2024, 1, 2),
            datetime.date(2024, 1, 3),
            datetime.date(2024, 1, 4),
        ],
        "confirmed": [True, False, True, False],
        "measure_col": [1.12, 10, None, 35],
    }
)   

df = pd.DataFrame(
    {
        "country_id": [1, 1, 2, 3],
        "country_name": ["Italy", "Italy", "Egypt", "France"],
        "another_column": ["dog", "cat", "pigeon", "opossum"],
        "date_column": [
            datetime.date(2024, 1, 1),
            datetime.date(2024, 1, 2),
            datetime.date(2024, 1, 3),
            datetime.date(2024, 1, 4),
        ],
        "male": [True, False, True, False],
        "measure_col": [1.12, 10, None, 35],
    }
)

workspace = dwh.Workspace("pypd_dwh_test", "Library fao_pypd_dwh test workspace")

schema = dwh.Schema(df, "a_schema", "A test schema")
schema.set_dimensions([
    dwh.Dimension(df[["country_id", "country_name"]], index_column="country_id", labels_column="country_name"), 
    dwh.Dimension(df.date_column, role="time"), 
    "male"
    ])
schema.set_measures([dwh.Measure(df.measure_col, label="Measure Label", unit="kg", precision=2)])

workspace.add_schema(schema)
workspace.to_dwh()
```

# Installation
```
pip install git+https://github.com/un-fao/fao_pypd_dwh.git@main#egg=fao_pypd_dwh
```

Requires python >= 3.11
