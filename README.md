This library adds some barebones functionality to remove a lot of boilerplate code from ETL/ELT pipelines for the fao_dwh_review project

How to use it:
- Create a Schema object from your dataframe
- Use the set_dimensions() and set_measures() methods. You can either pass strings or use Dimension/Measure objects for more control.
- Upload the jsonstats to the DWH with schema's to_dwh() method