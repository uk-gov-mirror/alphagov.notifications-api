import pytest
from flask import json


@pytest.mark.parametrize('path', ['/', '/_status'])
def test_get_status_all_ok(client, notify_db_session, path):
    response = client.get(path)
    assert response.status_code == 200
    resp_json = json.loads(response.get_data(as_text=True))
    assert resp_json['status'] == 'ok'
    assert resp_json['db_version']
    assert resp_json['travis_commit']
    assert resp_json['travis_build_number']
    assert resp_json['build_time']
