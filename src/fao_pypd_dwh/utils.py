from .constants import API_BASE

import requests
import datetime
import logging
import json

import pandas as pd

logger = logging.getLogger(__name__)

def to_string(value) -> str:
    if pd.isna(value):
        raise ValueError("Index column cannot be NaN")
    if isinstance(value, datetime.datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%S")
    elif isinstance(value, datetime.date):
        return value.strftime("%Y-%m-%d")
    else:
        return str(value)


def upload_workspace(id: str, label: str, source: str|None = None, note: list[str]|None = None):
    jsonstat_dict = {
        "version": "2.0",
        "class": "collection",
        "label": label,
        "updated": datetime.datetime.now().strftime("%Y-%m-%d"),
        "source": source,
        "note": note,
        "extension": {"resource_id": id},
    }
    logger.info(json.dumps(jsonstat_dict))
    res = requests.get(f"{API_BASE}/workspaces/{id}")
    if res.status_code == 200:
        post_res = requests.put(f"{API_BASE}/workspaces/{id}", json=jsonstat_dict)
        post_res.raise_for_status()
        return id
    elif res.status_code == 404:
        return id
    else:
        raise Exception(f"Error checking workspace existence: {res.status_code} - {res.text}")

def prepare_column_to_dict(data: pd.Series):
    #replace with None
    data = data.astype(object).where(pd.notna(data), None)
    return data.to_dict()    


def upload_dimesion(
    data: pd.DataFrame | pd.Series,
    workspace_id: str,
    dimension_id: str,
    dimension_label: str,
    role: str | None = None,
    index_column: str | None = None,
    labels_column: str | None = None,
):
    if index_column is None:
        index_column = dimension_id   

    if isinstance(data, pd.DataFrame):
        if index_column not in data.columns:
            raise ValueError(f"Index column {index_column} does not exist in the provided DataFrame")
        if labels_column is not None and not labels_column in data.columns:
            raise ValueError(f"Labels column {labels_column} does not exist in the provided DataFrame")

    jsonstat_dict = {"version": "2.0", "class": "dimension", "label": dimension_label}

    if isinstance(data, pd.DataFrame):
        if index_column is None:
            raise ValueError("index_column must be provided when data is a DataFrame.")
        jsonstat_dict["category"] = {"index": data[index_column].tolist()}
        if labels_column:
            jsonstat_dict["category"]["label"] = prepare_column_to_dict(data.set_index(index_column)[labels_column])
    else:
        jsonstat_dict["category"] = {"index": data.tolist()}

    jsonstat_dict["extension"] = {
        "resource_id": (
            dimension_id if not dimension_id.startswith("dim_") else dimension_id[4:]
        ),
        "referenced": False,
        "referenced_by": [],
        "additional_bq_fields": {}
    }

    if role:
        jsonstat_dict["extension"]["role"] = role

    if isinstance(data, pd.DataFrame):
        for col in data.columns:
            if col != index_column and col != labels_column:
                jsonstat_dict["extension"]["additional_bq_fields"][col] = prepare_column_to_dict(data[[index_column, col]].set_index(index_column)[col])

    res = requests.get(f"{API_BASE}/workspaces/{workspace_id}/dimensions/{dimension_id}")
    if res.status_code == 404:
        references = None
    elif res.status_code == 200:
        references = res.json().get("extension", {}).get("references", None)
    else:
        raise Exception(f"Error checking dimension {workspace_id}/{dimension_id} existence: {res.status_code} - {res.text}")

    if references:
        jsonstat_dict["extension"]["referenced"] = True
        jsonstat_dict["extension"]["referenced_by"] = references

    logger.info(json.dumps(jsonstat_dict))
    if res.status_code == 404:
        res_post = requests.post(
            f"{API_BASE}/workspaces/{workspace_id}/dimensions",
            json=jsonstat_dict
        )
        res_post.raise_for_status()
    else:
        res_put = requests.put(
            f"{API_BASE}/workspaces/{workspace_id}/dimensions/{dimension_id}",
            json=jsonstat_dict,
        )
        res_put.raise_for_status()


def upload_measure(
    workspace_id: str,
    measure_id: str,
    measure_label: str,
    unit: str|None = None,
    precision: int|None = None,
    min = None,
    max = None,
    nodata = None,
    aggregator: str|None = 'SUM',
):

    res = requests.get(f"{API_BASE}/workspaces/{workspace_id}/measures/{measure_id}")
    if res.status_code == 200:
        logger.info(f"Measure {workspace_id}/{measure_id} already exists")
        return

    jsonstat_dict = {
        "version": "2.0", 
        "class": "dimension",
        "category": {
            "label":{
                measure_id: measure_label
                },
            }
        }
    if unit:
        jsonstat_dict["category"]["unit"] = {measure_id: {"label": unit, "decimals": 1}}

    jsonstat_dict["extension"] = {
        "constraints": {
            measure_id: {
                "precision": precision,
                "min": min,
                "max": max,
                "nodata": nodata
            }
        },
        "aggregator": {
            measure_id: aggregator
        },
        "resource_id": measure_id,
    }

    logger.info(json.dumps(jsonstat_dict))
    res_post = requests.post(
        f"{API_BASE}/workspaces/{workspace_id}/measures",
        json=jsonstat_dict
    )
    res_post.raise_for_status()


def upload_schema(
    workspace_id: str,
    schema_id: str,
    schema_label: str,
    dimension_ids: list[str],
    measure_ids: list[str],
    time_dims: list[str],
    geo_dims: list[str],
    additional_bq_fields: list[str],
):
    res = requests.get(f"{API_BASE}/workspaces/{workspace_id}/schemas/{schema_id}")
    if res.status_code == 200:
        logger.info(f"Schema {workspace_id}/{schema_id} already exists")
        return

    jsonstat_dict = {
        "version": "2.0", 
        "class": "dataset",
        "label": schema_label,
        "id": dimension_ids + ["measures"],
        "size": [1] * (len(dimension_ids) + 1),
        "role": {
            "time": time_dims,
            "geo": geo_dims,
            "metric":["measures"]
        },
        "value": [],
        "dimension": {id: {"href":f"{API_BASE}/workspaces/{workspace_id}/dimensions/{id}"} for id in dimension_ids}
            | {"measures": {"href":f"{API_BASE}/workspaces/{workspace_id}/measures:combine?{'&'.join([f'measure_ids={id}' for id in measure_ids])}"}},
        "extension": {
            "resource_id": schema_id,
            "additional_bq_fields":{}
        }
    }
    for col in additional_bq_fields:
        jsonstat_dict["extension"]["additional_bq_fields"][col] = {}

    logger.info(json.dumps(jsonstat_dict))
    res_post = requests.post(
        f"{API_BASE}/workspaces/{workspace_id}/schemas", json=jsonstat_dict
    )
    res_post.raise_for_status()
