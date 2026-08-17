import json

import boto3
import pytest
from moto import mock_aws

from ingestion.sinks import JsonlFileSink, KinesisSink, StdoutSink, build_sink


def test_stdout_sink_prints_json_line(capsys):
    sink = StdoutSink()
    sink.write({"mmsi": 123, "lat": 1.0})
    captured = capsys.readouterr()
    assert json.loads(captured.out.strip()) == {"mmsi": 123, "lat": 1.0}


def test_jsonl_file_sink_appends_lines(tmp_path):
    path = tmp_path / "out.jsonl"
    sink = JsonlFileSink(str(path))
    sink.write({"mmsi": 1})
    sink.write({"mmsi": 2})
    sink.close()

    lines = path.read_text().strip().split("\n")
    assert [json.loads(l)["mmsi"] for l in lines] == [1, 2]


@mock_aws
def test_kinesis_sink_writes_records_to_real_stream_api():
    region = "us-east-1"
    client = boto3.client("kinesis", region_name=region)
    client.create_stream(StreamName="test-stream", ShardCount=1)

    sink = KinesisSink(stream_name="test-stream", region_name=region, client=client)
    sink.write({"mmsi": 987654321, "port_zone": "USLAX"})

    shard_id = client.describe_stream(StreamName="test-stream")["StreamDescription"]["Shards"][0]["ShardId"]
    iterator = client.get_shard_iterator(
        StreamName="test-stream", ShardId=shard_id, ShardIteratorType="TRIM_HORIZON"
    )["ShardIterator"]
    records = client.get_records(ShardIterator=iterator)["Records"]

    assert len(records) == 1
    payload = json.loads(records[0]["Data"])
    assert payload["mmsi"] == 987654321
    assert records[0]["PartitionKey"] == "987654321"


@mock_aws
def test_kinesis_sink_write_batch_uses_put_records():
    region = "us-east-1"
    client = boto3.client("kinesis", region_name=region)
    client.create_stream(StreamName="test-stream", ShardCount=1)

    sink = KinesisSink(stream_name="test-stream", region_name=region, client=client)
    response = sink.write_batch([{"mmsi": 1}, {"mmsi": 2}, {"mmsi": 3}])

    assert response["FailedRecordCount"] == 0
    assert len(response["Records"]) == 3


def test_build_sink_rejects_unknown_kind():
    with pytest.raises(ValueError):
        build_sink("carrier-pigeon")
