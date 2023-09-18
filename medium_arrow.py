import os
import json
import boto3
import logging
import urllib.parse
import awswrangler as wr
import pandas as pd
from datetime import datetime
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def s3_url(bucket, prefix, folder=False):
    """Produce s3 url from 'bucket' and 'prefix'"""
    assert bucket and prefix
    return "s3://{bucket}/{prefix}{folder}".format(
        bucket=bucket, prefix=prefix, folder="/" if folder else ""
    )


def read_parquet(s3_path, columns, source, dataset=True):
    """Read Apache Parquet file(s) from a received S3
    prefix or list of S3 objects paths.
    Convert the last_updated column from Timestamp to Epoch time.
    Return records as list of dictionary.
    """
    assert source, "source can't be None"
    df = wr.s3.read_parquet(path=s3_path, columns=columns, dataset=dataset)
    df[["last_updated"]] = (
        df[["last_updated"]] - pd.Timestamp("1970-01-01")
    ) // pd.Timedelta("1s")
    df["source"] = source
    df["location"] = s3_path

    return df.to_dict(orient="records")


def write_to_ddb(client, table_name, records):
    """Write data to DynamoDB table.
    Returns inserted row count.
    """
    table = client.Table(table_name)
    row_count = 0
    for row in records:
        table.put_item(Item=row)
        row_count += 1
    return row_count


def lambda_handler(event, context):
    record = event["Records"][0]
    assert "eventSource" in record
    assert "eventName" in record
    assert record["eventSource"] == "aws:s3", (
        "Unknown event source: %s." % record["eventSource"]
    )
    assert record["eventName"].startswith("ObjectCreated:"), (
        "Unsupported event name: %s." % record["eventName"]
    )
    assert "s3" in record
    assert "bucket" in record["s3"]
    assert "name" in record["s3"]["bucket"]
    assert "object" in record["s3"]
    assert "key" in record["s3"]["object"]
    bucket = record["s3"]["bucket"][
        "name"
    ]  # If S3 URL contains special character usch as '=', it will be quoted, like: %3D
    # This is to unquote them back to original character, or it will complain path not exist.
    key = urllib.parse.unquote(record["s3"]["object"]["key"])  # create s3 path
    s3_path = s3_url(
        bucket=bucket, prefix=key
    )  # Retrieving the data directly from Amazon S3
    cols = ["ticker_symbol", "sector", "last_updated"]

    df = read_parquet(
        s3_path=s3_path, columns=cols, source="system", dataset=True
    )  # Instantiate Dynamodb connection
    ddb = boto3.resource("dynamodb")  # Get the dynamodb table name
    ddb_table = os.environ["DDB_TABLE"]  # Write data to dynamodb
    record_count = write_to_ddb(
        client=ddb, table_name=ddb_table, records=df
    )  # return total record inserted
    return ("Total records inserted:", record_count)
