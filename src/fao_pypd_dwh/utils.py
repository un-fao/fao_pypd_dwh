from .constants import *

import requests
import datetime
import logging
import json
import os
import sys

import pandas as pd

logger = logging.getLogger(__name__)

def to_string(value) -> str:
    if pd.isna(value):
        return None
    elif isinstance(value, datetime.datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%S")
    elif isinstance(value, datetime.date):
        return value.strftime("%Y-%m-%d")
    else:
        return str(value)


def prepare_column_to_dict(data: pd.Series):
    # replace with None
    data = data.astype(object).where(pd.notna(data), None)
    return data


def upload_workspace(id: str, label: str, source: str|None = None, note: list[str]|None = None, environment: str = "review"):
    if environment in ("review", "rev", "fao-dwh-review"):
        api_base = API_BASE_REVIEW
    elif environment in ("prod", "production", "fao-dwh"):
        api_base = API_BASE_PROD
    else:
        raise ValueError(f"Unknown environment: {environment}. Please use 'review' or 'production'.")

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
    res = requests.get(f"{api_base}/workspaces/{id}")
    if res.status_code == 200:
        post_res = requests.put(f"{api_base}/workspaces/{id}", json=jsonstat_dict)
        post_res.raise_for_status()
        return id
    elif res.status_code == 404:
        post_res = requests.post(f"{api_base}/workspaces", json=jsonstat_dict)
        post_res.raise_for_status()
        return id
    else:
        raise Exception(f"Error checking workspace existence: {res.status_code} - {res.text}")


def dimension_exists(
    workspace_id: str, dimension_id: str, environment: str = "review"
) -> bool:
    if environment in ("review", "rev", "fao-dwh-review"):
        api_base = API_BASE_REVIEW
    elif environment in ("prod", "production", "fao-dwh"):
        api_base = API_BASE_PROD
    else:
        raise ValueError(
            f"Unknown environment: {environment}. Please use 'review' or 'production'."
        )

    res = requests.get(
        f"{api_base}/workspaces/{workspace_id}/dimensions/{dimension_id}"
    )
    if res.status_code == 200:
        return True
    elif res.status_code == 404:
        return False
    else:
        raise Exception(
            f"Error checking dimension existence: {res.status_code} - {res.text}"
        )


def upload_dimesion(
    data: pd.DataFrame | pd.Series,
    workspace_id: str,
    dimension_id: str,
    dimension_label: str,
    role: str | None = None,
    index_column: str | None = None,
    labels_column: str | None = None,
    parents_column: str | None = None,
    merge_members: bool = False,
    environment: str = "review",
):

    if environment in ("review", "rev", "fao-dwh-review"):
        api_base = API_BASE_REVIEW
    elif environment in ("prod", "production", "fao-dwh"):
        api_base = API_BASE_PROD
    else:
        raise ValueError(f"Unknown environment: {environment}. Please use 'review' or 'production'.")

    if index_column is None:
        index_column = dimension_id   

    if isinstance(data, pd.DataFrame):
        if index_column not in data.columns:
            raise ValueError(f"Index column {index_column} does not exist in the provided DataFrame")
        if labels_column is not None and not labels_column in data.columns:
            raise ValueError(f"Labels column {labels_column} does not exist in the provided DataFrame")
        if parents_column is not None and not parents_column in data.columns:
            raise ValueError(f"Parents column {parents_column} does not exist in the provided DataFrame")

    exists = dimension_exists(workspace_id, dimension_id, environment)

    jsonstat_dict = {"label": dimension_label}

    if isinstance(data, pd.DataFrame):
        if index_column is None:
            raise ValueError("index_column must be provided when data is a DataFrame.")
        jsonstat_dict["category"] = {"index": data[index_column].tolist()}
        if labels_column:
            jsonstat_dict["category"]["label"] = prepare_column_to_dict(data.set_index(index_column)[labels_column]).to_dict()
        if parents_column:
            hierarchy = data[[index_column, parents_column]].explode(column=parents_column)
            hierarchy = hierarchy[hierarchy[parents_column].notna()]
            if hierarchy[parents_column].dtype != hierarchy[index_column].dtype:
                raise ValueError("The type of parents_column must be the same as the type of index_column or a tuple of that same type.")
            if not hierarchy[parents_column].isin(data[index_column]).all():
                raise ValueError("All values in parents_column must be in index_column.")
            jsonstat_dict["category"]["child"] = prepare_column_to_dict(hierarchy.groupby(parents_column)[index_column].apply(list)).to_dict()
    else:
        jsonstat_dict["category"] = {"index": data.tolist()}

    jsonstat_dict["extension"] = {
        "additional_bq_fields": {}
    }

    if not merge_members or not exists:
        jsonstat_dict["extension"]["resource_id"] = dimension_id
        if role:
            jsonstat_dict["extension"]["role"] = role

    if isinstance(data, pd.DataFrame):
        for col in data.columns:
            if col != index_column and col != labels_column and col != parents_column:
                jsonstat_dict["extension"]["additional_bq_fields"][col] = prepare_column_to_dict(data[[index_column, col]].set_index(index_column)[col]).to_dict()

    logger.info(json.dumps(jsonstat_dict))

    if exists:
        if merge_members:
            res = requests.patch(
                f"{api_base}/workspaces/{workspace_id}/dimensions/{dimension_id}",
                json=jsonstat_dict
            )
            res.raise_for_status()
        else:
            res = requests.put(
                f"{api_base}/workspaces/{workspace_id}/dimensions/{dimension_id}",
                json=jsonstat_dict
            )
            res.raise_for_status()
    else:
        res = requests.post(
            f"{api_base}/workspaces/{workspace_id}/dimensions",
            json=jsonstat_dict,
        )
        res.raise_for_status()


def upload_measure(
    workspace_id: str,
    measure_id: str,
    measure_label: str,
    unit: str | None = None,
    precision: int | None = None,
    min=None,
    max=None,
    nodata=None,
    aggregator: str | None = "SUM",
    touch_if_exists: bool = False,
    environment: str = "review",
):
    if environment in ("review", "rev", "fao-dwh-review"):
        api_base = API_BASE_REVIEW
    elif environment in ("prod", "production", "fao-dwh"):
        api_base = API_BASE_PROD
    else:
        raise ValueError(
            f"Unknown environment: {environment}. Please use 'review' or 'production'."
        )

    res = requests.get(f"{api_base}/workspaces/{workspace_id}/measures/{measure_id}")
    if res.status_code == 200:
        logger.info(f"Measure {workspace_id}/{measure_id} already exists")
        if touch_if_exists:
            res = requests.patch(
                f"{api_base}/workspaces/{workspace_id}/measures/{measure_id}",
                json={"extension": {}},
            )
            res.raise_for_status()
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
        f"{api_base}/workspaces/{workspace_id}/measures",
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
    touch_if_exists: bool = False,
    environment: str = "review",
):
    
    if environment in ("review", "rev", "fao-dwh-review"):
        api_base = API_BASE_REVIEW
    elif environment in ("prod", "production", "fao-dwh"):
        api_base = API_BASE_PROD
    else:
        raise ValueError(
            f"Unknown environment: {environment}. Please use 'review' or 'production'."
        )
        
    res = requests.get(f"{api_base}/workspaces/{workspace_id}/schemas/{schema_id}")
    if res.status_code == 200:
        logger.info(f"Schema {workspace_id}/{schema_id} already exists")
        if touch_if_exists:
            res = requests.patch(
                f"{api_base}/workspaces/{workspace_id}/schemas/{schema_id}",
                json={"extension": {}}
            )
            res.raise_for_status()
        return

    jsonstat_dict = {
        "version": "2.0", 
        "class": "dataset",
        "label": schema_label,
        "id": dimension_ids + (["measures"] if measure_ids else []),
        "size": [1] * (len(dimension_ids) + (1 if measure_ids else 0)),
        "role": {
            "time": time_dims,
            "geo": geo_dims,
            "metric":["measures"] if measure_ids else []
        },
        "value": [],
        "dimension": {id: {"href":f"{api_base}/workspaces/{workspace_id}/dimensions/{id}"} for id in dimension_ids}
            | ({"measures": {"href":f"{api_base}/workspaces/{workspace_id}/measures:combine?{'&'.join([f'measure_ids={id}' for id in measure_ids])}"}} if measure_ids else {}),
        "extension": {
            "resource_id": schema_id,
            "additional_bq_fields":{}
        }
    }
    for col in additional_bq_fields:
        jsonstat_dict["extension"]["additional_bq_fields"][col] = {}

    logger.info(json.dumps(jsonstat_dict))
    res_post = requests.post(
        f"{api_base}/workspaces/{workspace_id}/schemas", json=jsonstat_dict
    )
    res_post.raise_for_status()


def upload_data_to_bucket(
    workspace_id: str,
    schema_id: str,
    data: pd.DataFrame,
    mode: str | None = "replace",
    rows_per_file: int | None = None,
    ingestion_id: str | None = None,
    environment: str = "review",
):
    if mode not in ("replace", "append", "chunking", None):
        raise ValueError(f"Unknown mode: {mode}. Please use 'replace', 'append' or 'chunking'.")

    if environment in ("review", "rev", "fao-dwh-review"):
        bucket_prefix = BUCKET_PREFIX_REVIEW
        project_id = GCP_PROJECT_REVIEW
    elif environment in ("prod", "production", "fao-dwh"):
        bucket_prefix = BUCKET_PREFIX_PROD
        project_id = GCP_PROJECT_PROD
    else:
        raise ValueError(f"Unknown environment: {environment}. Please use 'review' or 'production'.")

    try:
        from google.cloud import storage, bigquery
    except ImportError:
        raise ImportError("google-cloud-storage and google-cloud-bigquery libraries are required to upload data to bucket. Please install them.")

    gcs_client = storage.Client()
    bq_client = bigquery.Client(project=project_id)

    try:
        query = f"SELECT DISTINCT _data_ingestion_id FROM `{project_id}.{workspace_id}.fct_{schema_id}`"
        query_job = bq_client.query(query)
        old_ingestion_ids = [row["_data_ingestion_id"] for row in query_job.result()]
    except Exception:
        old_ingestion_ids = []

    upload_bucket = gcs_client.bucket(f"{bucket_prefix}-{workspace_id}")

    if mode is None:
        mode = "replace" if len(old_ingestion_ids) == 1 else "append"
    elif mode == "replace":
        if len(old_ingestion_ids) == 0:
            mode = "append"
        elif len(old_ingestion_ids) > 1:
            raise ValueError(f"Cannot replace data for schema {schema_id} in workspace {workspace_id} because there are multiple unique _data_ingestion_id values in the BigQuery table. Please use mode='append' or clean the table before using mode='replace'.")

    if mode == "replace":
        if ingestion_id:
            file_name = f"{ingestion_id}.csv"
        else:
            file_name = f"{old_ingestion_ids[0]}.csv"
    elif mode == "append":
        if ingestion_id:
            file_name = f"{ingestion_id}.csv"
        else:
            file_name = f"{schema_id}::part-{len(old_ingestion_ids)}.csv"

    if mode != "chunking":
        csv = data.to_csv(index=False)
        blob = upload_bucket.blob(f"data/{schema_id}/{file_name}")
        blob.upload_from_string(csv)
        logger.info(f"Uploaded {file_name} to bucket {bucket_prefix}-{workspace_id}")
    else:
        n_chunks = len(old_ingestion_ids)
        if n_chunks == 0:
            n_chunks = 1
        if len(data) < n_chunks:
            raise ValueError(f"Cannot split data into {n_chunks} files because there are only {len(data)} rows. Please use a different mode or verify data integrity.")
        if rows_per_file is None:
            rows_per_file = -(len(data) // -n_chunks)  # round up
        elif len(data) / n_chunks < rows_per_file:
            rows_per_file = -(len(data) // -n_chunks) # round up
            logger.info(f"Adjusting rows_per_file to {rows_per_file} to fit the data into at least {n_chunks} files.")
        start = 0
        part = 0
        while True:
            if start >= len(data):
                break
            data_part = data.iloc[start : min(len(data), start + rows_per_file)]
            csv = data_part.to_csv(index=False)
            if sys.getsizeof(csv) <= MAX_BUCKET_FILE_SIZE:
                file_name = f"{schema_id}::part-{part}.csv"
                blob = upload_bucket.blob(f"data/{schema_id}/{file_name}")
                blob.upload_from_string(csv)
                logger.info(f"Uploaded {file_name} to bucket {bucket_prefix}-{workspace_id}")
                start += rows_per_file
                part += 1
            elif rows_per_file <= 1:
                raise Exception(
                    f"Cannot split data into smaller files. Row size is too big."
                )
            else:
                rows_per_file = rows_per_file // 2
