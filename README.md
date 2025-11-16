This library adds some barebones functionality to remove a lot of boilerplate code from ETL/ELT pipelines for the fao_dwh_review project

How to use it:
- Create a Schema object from your dataframe
- Use the set_dimensions() and set_measures() methods. You can either pass strings or use Dimension/Measure objects for more control.
- Upload the jsonstats to the DWH with schema's to_dwh() method

```
import fao_pypd_dwh as dwh
import pandas as pd
import datetime

df = pd.DataFrame({"first_column": [1,2,3],
                "another_column":["one","two","three"],
                "date_column":[datetime.date(2024,1,1), datetime.date(2024,1,2), datetime.date(2024,1,3)],
                "measure_col":[1.12, 10, None]})

schema = dwh.Schema(df, "a_schema", "A test schema")

schema.set_dimensions(["first_column", dwh.Dimension(df.date_column, role="time")])

schema.set_measures([dwh.Measure(df.measure_col, label="Some measure", unit="kg", precision=2)])

schema.to_dwh("your_workspace_id")
```
