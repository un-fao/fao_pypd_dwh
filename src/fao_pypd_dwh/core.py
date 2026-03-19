from . import utils

from typing import Self
import time

import pandas as pd


class Dimension:
    def __init__(
        self,
        data: pd.DataFrame | pd.Series,
        id: str | None = None,
        label: str | None = None,
        role: str | None = None,
        index_column: str | None = None,
        labels_column: str | None = None,
    ):
        self.data = data
        if id is None:
            if isinstance(data, pd.Series):
                if data.name is not None:
                    id = str(data.name)
                else:
                    raise ValueError("Dimension id cannot be None if data is a Series without a name.")
            elif index_column:
                id = index_column
            else:
                raise ValueError("id and index_column cannot both be None if data is a DataFrame.")
        self.id = id
        
        if label is None:
            label = self.id
        self.label = label
        
        if role not in (None, "time", "geo"):
            raise ValueError("role must be 'time', 'geo' or None")
        self.role = role
        
        if index_column is None:
            if isinstance(data, pd.Series):
                index_column = data.name
            else:
                raise ValueError("index_column must be provided when data is a DataFrame.")
        self.index_column = index_column
        
        self.labels_column = labels_column

    def to_dwh(self, workspace_id: str, environment: str = "review"):
        copy = self.data.copy()
        if isinstance(copy, pd.Series):
            copy = copy.drop_duplicates().sort_values()
            if copy.isna().any():
                raise ValueError(f"Error: dimension '{self.id}' contains null values")
            copy = copy.apply(utils.to_string)
        else:
            copy = copy.drop_duplicates().sort_values(by=self.index_column)

            # Check if index_column is unique key for dimension
            if copy[self.index_column].isna().any():
                raise ValueError(f"Error: column '{self.index_column}' contains null values")
            independant_cols = []
            for col in copy.columns:
                if col != self.index_column:
                    if not copy.groupby(self.index_column)[col].nunique().le(1).all():
                        independant_cols.append(col)
            if independant_cols:
                raise ValueError(f"Error: columns {independant_cols} do not fully depend on index column '{self.index_column}'")

            copy[self.index_column] = copy[self.index_column].apply(utils.to_string)
            for col in copy.select_dtypes(exclude=['number', bool]).columns:
                copy[col] = copy[col].apply(utils.to_string)
                
        utils.upload_dimesion(
            data=copy,
            workspace_id=workspace_id,
            dimension_id=self.id,
            dimension_label=self.label,
            role=self.role,
            index_column=self.index_column,
            labels_column=self.labels_column,
            environment=environment,
        )

    def update_members(self, workspace_id: str, environment: str = "review"):
        if not utils.dimension_exists(workspace_id, self.id, environment=environment):
            self.to_dwh(workspace_id, environment=environment)
            return
        
        copy = self.data.copy()
        if isinstance(copy, pd.Series):
            copy = copy.drop_duplicates().sort_values()
            if copy.isna().any():
                raise ValueError(f"Error: dimension '{self.id}' contains null values")
            copy = copy.apply(utils.to_string)
        else:
            copy = copy.drop_duplicates().sort_values(by=self.index_column)

            # Check if index_column is unique key for dimension
            if copy[self.index_column].isna().any():
                raise ValueError(f"Error: column '{self.index_column}' contains null values")
            independant_cols = []
            for col in copy.columns:
                if col != self.index_column:
                    if not copy.groupby(self.index_column)[col].nunique().le(1).all():
                        independant_cols.append(col)
            if independant_cols:
                raise ValueError(f"Error: columns {independant_cols} do not fully depend on index column '{self.index_column}'")

            copy[self.index_column] = copy[self.index_column].apply(utils.to_string)
            for col in copy.select_dtypes(exclude=['number', bool]).columns:
                copy[col] = copy[col].apply(utils.to_string)
                
        utils.merge_dimesion_members(
            data=copy,
            workspace_id=workspace_id,
            dimension_id=self.id,
            index_column=self.index_column,
            labels_column=self.labels_column,
            environment=environment,
        )


class Measure:
    def __init__(
        self,
        data: pd.Series | None = None,
        id: str | None = None,
        label: str | None = None,
        unit: str|None = None,
        precision: int|None = None,
        min = None,
        max = None,
        nodata = None,
        aggregator: str|None = 'SUM',
    ):
        self.data = data
        if id is None:
            if data is not None and data.name is not None:
                id = data.name
            else:
                raise ValueError("An id must be specified")
        self.id = id
        if label is None:
            label = id
        self.label = label
        self.unit = unit
        self.precision = precision
        self.min = min
        self.max = max
        self.nodata = nodata
        self.aggregator = aggregator

    def to_dwh(self, workspace_id: str, environment: str = "review"):
        utils.upload_measure(
            workspace_id,
            self.id,
            self.label,
            self.unit,
            self.precision,
            self.min,
            self.max,
            self.nodata,
            self.aggregator,
            environment=environment,
        )


class Schema:

    def __init__(self, df:pd.DataFrame, id:str, label:str|None = None):
        self.df = df
        self.id = id
        if label is None:
            label = id
        self.label = label
        self.dimensions = []
        self.measures = []
        
        self.data_upload_params = None
        
        self._append_owner_dimensions = None
        self._append_owner_measures = None

    def set_dimensions(self, dimensions:list[Dimension|str]) -> Self:
        for dim in dimensions:
            if isinstance(dim, str):
                dim = Dimension(data=self.df[dim])
            self.dimensions.append(dim)
            if self._append_owner_dimensions is not None:
                self._append_owner_dimensions(dim)
        return self

    def set_measures(self, measures:list[Measure|str]) -> Self:
        for measure in measures:
            if isinstance(measure, str):
                measure = Measure(data=self.df[measure])
            self.measures.append(measure)
            if self._append_owner_measures is not None:
                self._append_owner_measures(measure)
        return self

    def _sync(self):
        if self._append_owner_dimensions is not None:
            self._append_owner_dimensions(*self.dimensions)
        if self._append_owner_measures is not None:
            self._append_owner_measures(*self.measures)

    def to_dwh(self, workspace_id: str, environment: str = "review") -> Self:
        dim_ids = [i.id for i in self.dimensions]
        mes_ids = [i.id for i in self.measures]
        additional = []
        for col in self.df.columns:
            is_used = False
            for dim in self.dimensions:
                if isinstance(dim.data, pd.Series):
                    if col == dim.data.name:
                        is_used = True
                        break
                else:
                    if col in dim.data.columns:
                        is_used = True
                        break
            for mes in self.measures:
                if col == mes.id:
                    is_used = True
                    break
            if not is_used:
                additional.append(col)

        utils.upload_schema(
            workspace_id,
            self.id,
            self.label,
            dim_ids,
            mes_ids,
            [i.id for i in self.dimensions if i.role == "time"],
            [i.id for i in self.dimensions if i.role == "geo"],
            additional,
            environment=environment,
        )
        return self
    
    def set_data_upload_params(self, mode: str | None = "replace", rows_per_file: int | None = None) -> Self:
        self.data_upload_params = {
            "mode": mode,
            "rows_per_file": rows_per_file,
        }
        return self
    
    def upload_data(self, workspace_id: str, mode: str | None = None, rows_per_file: int | None = None, environment: str = "review") -> Self:
        columns_to_drop = []
        for col in self.df.columns:
            for dim in self.dimensions:
                if isinstance(dim.data, pd.DataFrame) and col != dim.index_column and col in dim.data.columns:
                    columns_to_drop.append(col)
                    break
                
        data = self.df.drop(columns=columns_to_drop).copy()
        
        data.rename(columns={dim.index_column: dim.id for dim in self.dimensions}, inplace=True)
        
        if mode is None and self.data_upload_params is not None:
            mode = self.data_upload_params.get("mode", "replace")
        if rows_per_file is None and self.data_upload_params is not None:
            rows_per_file = self.data_upload_params.get("rows_per_file", None)

        utils.upload_data_to_bucket(
            workspace_id,
            self.id,
            data,
            mode=mode,
            rows_per_file=rows_per_file,
            environment=environment,
        )
        return self


class Workspace:

    def __init__(
        self,
        id: str,
        label: str,
        source: str | None = None,
        note: list[str] | None = None,
        environment: str = "review",
    ):
        self.id = id
        self.label = label
        self.source = source
        self.note = note
        self.environment = environment

        self.dimensions = {}
        self.measures = {}
        self.schemas = {}

    def add_schema(self, *schemas: Schema) -> Self:
        for schema in schemas:
            schema._append_owner_dimensions = self.add_dimension
            schema._append_owner_measures = self.add_measure
            schema._sync()
            self.schemas[schema.id] = schema
        return self

    def remove_schema(self, schema_id: str) -> Self:
        self.schemas[schema_id]._append_owner_dimensions = None
        self.schemas[schema_id]._append_owner_measures = None
        del self.schemas[schema_id]
        return self

    def add_dimension(self, *dimensions: Dimension) -> Self:
        for dimension in dimensions:
            self.dimensions[dimension.id] = dimension
        return self

    def remove_dimension(self, dimension_id: str) -> Self:
        del self.dimensions[dimension_id]
        return self

    def add_measure(self, *measures: Measure) -> Self:
        for measure in measures:
            self.measures[measure.id] = measure
        return self

    def remove_measure(self, measure_id: str) -> Self:
        del self.measures[measure_id]
        return self

    def to_dwh(self) -> Self:
        utils.upload_workspace(self.id, self.label, self.source, self.note)
        for dim in self.dimensions.values():
            dim.to_dwh(self.id, self.environment)
        for measure in self.measures.values():
            measure.to_dwh(self.id, self.environment)
        time.sleep(3)
        for schema in self.schemas.values():
            schema.to_dwh(self.id, self.environment)
        return self
    
    def upload_data(self, mode: str | None = None, rows_per_file: int | None = None) -> Self:
        for schema in self.schemas.values():
            schema.upload_data(self.id, mode=mode, rows_per_file=rows_per_file, environment=self.environment)
