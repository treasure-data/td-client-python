import pytest

from tdclient.util import normalize_connector_config


def test_normalize_connector_config():
    config = {"in": {"type": "s3"}}
    assert normalize_connector_config(config) == {
        "in": {"type": "s3"},
        "out": {},
        "exec": {},
        "filters": [],
    }


def test_normalize_connector_config_with_expected_keys():
    config = {
        "in": {"type": "s3"},
        "out": {"mode": "append"},
        "exec": {"guess_plugins": ["json"]},
        "filters": [{"type": "speedometer"}],
    }
    assert normalize_connector_config(config) == config


def test_nomalize_connector_with_config_key():
    config = {
        "config": {
            "in": {"type": "s3"},
            "out": {"mode": "append"},
            "exec": {"guess_plugins": ["json"]},
            "filters": [{"type": "speedometer"}],
        }
    }
    assert normalize_connector_config(config) == config["config"]


def test_normalize_connector_without_in_key():
    config = {"config": {"type": "s3"}}

    assert normalize_connector_config(config) == {
        "in": config["config"],
        "out": {},
        "exec": {},
        "filters": [],
    }


def test_normalize_connector_witout_in_and_config_keys():
    config = {"type": "s3"}
    assert normalize_connector_config(config) == {
        "in": config,
        "out": {},
        "exec": {},
        "filters": [],
    }


def test_normalize_conector_has_sibling_keys():
    config = {
        "config": {"in": {"type": "s3"}},
        "sibling_key": {"out": {"mode": "append"}},
    }

    with pytest.raises(ValueError):
        normalize_connector_config(config)
