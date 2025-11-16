from . import utils

import pandas as pd


def create_workspace(id: str, label: str, source: str|None = None, note: list[str]|None = None):
    return utils.create_workspace(id=id, label=label, source=source, note=note)

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
        if isinstance(data, pd.DataFrame) and index_column is None:
            raise ValueError("index_column must be provided when data is a DataFrame.")
        self.index_column = index_column
        self.labels_column = labels_column

    def to_dwh(self, workspace_id: str):
        copy = self.data.copy()
        if isinstance(copy, pd.Series):
            copy = copy.apply(utils.to_string)
        else:
            copy[self.index_column] = copy[self.index_column].apply(utils.to_string)
            for col in copy.select_dtypes(exclude=['number']).columns:
                copy[col] = copy[col].apply(utils.to_string)
        utils.upload_dimesion(
            data=copy,
            workspace_id=workspace_id,
            dimension_id=self.id,
            dimension_label=self.label,
            role=self.role,
            index_column=self.index_column,
            labels_column=self.labels_column,
        )

class Measure:
    def __init__(
        self,
        data: pd.Series,
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
            if data.name:
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
        
    def to_dwh(self, workspace_id: str):
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
        )

class Schema:
    dimensions = []
    measures = []
    def __init__(self, df:pd.DataFrame, id:str, label:str|None = None, workspace_id:str = None):
        self.df = df
        self.id = id
        if label is None:
            label = id
        self.label = label
        self.workspace_id = workspace_id

    def set_dimensions(self, dimensions:list[Dimension|str]):
        for dim in dimensions:
            if isinstance(dim, str):
                dim = Dimension(data=self.df[dim])
            self.dimensions.append(dim)

    def set_measures(self, measures:list[Measure|str]):
        for measure in measures:
            if isinstance(measure, str):
                measure = Measure(data=self.df[measure])
            self.measures.append(measure)

    def to_dwh(self, workspace_id: str|None = None):
        if workspace_id is None:
            if self.workspace_id is None:
                raise ValueError("A workspace_id must be specified")
            else:
                workspace_id = self.workspace_id
        for dim in self.dimensions:
            dim.to_dwh(workspace_id=workspace_id)
        for measure in self.measures:
            measure.to_dwh(workspace_id=workspace_id)
        dim_ids = [i.id for i in self.dimensions]
        mes_ids = [i.id for i in self.measures]
        utils.upload_schema(
            workspace_id,
            self.id,
            self.label,
            dim_ids,
            mes_ids,
            [i.id for i in self.dimensions if i.role == "time"],
            [i.id for i in self.dimensions if i.role == "geo"],
            [col for col in self.df.columns if col not in dim_ids and col not in mes_ids]
        )
