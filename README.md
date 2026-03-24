# Introduction
This library adds some barebones functionality to remove a lot of boilerplate code from ETL/ELT pipelines for the fao_dwh_review project

## How to use it
- Define your workspace
- Create a Schema object from your dataframe
- Use the set_dimensions() and set_measures() methods. You can either pass strings or use Dimension/Measure objects for more control.
- Upload the jsonstats to the DWH with workspace's to_dwh() method
- Optionally you may also upload your fact table(s) with the upload_data method. Use mode="append"|"replace"|"chunking".

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

workspace = dwh.Workspace("pypd_dwh_test", "Library fao_pypd_dwh test workspace")

schema = dwh.Schema(df, "a_schema", "A test schema")
schema.set_dimensions([
    dwh.Dimension(df[["country_id", "country_name"]], index_column="country_id", labels_column="country_name"), 
    dwh.Dimension(df.date_column, role="time"), 
    "confirmed"
    ])
schema.set_measures([dwh.Measure(df.measure_col, label="Measure Label", unit="kg", precision=2)])

workspace.add_schema(schema)
workspace.to_dwh()
workspace.upload_data()
```

While the workspace to_dwh method will upload all it's related contents, all the DWH objects can also be uploaded independently.
You may define the child-parents relationship of a dimension by passing the parents_column parameter in the Dimension definition. The parents column must contain objects of the same type as the index column or tuples/lists. A column of individual values mixed with tuples/lists is also accepted.
DataFrame/Series objects are passed by reference and not modified by this library. You may edit them after you define your DWH objects. Dimensions can be initialized using Series/DataFrames that originate from a different DataFrame than the one used to define the relative Schema. The library uses the column names to identify which columns are part of the a schema's defined dimensions or its additional fields.

# Installation
```
pip install git+https://github.com/un-fao/fao_pypd_dwh.git@main#egg=fao_pypd_dwh
```

Requires python >= 3.11
