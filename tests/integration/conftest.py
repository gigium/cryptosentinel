import pytest


def pytest_configure(config):
    config.addinivalue_line("markers", "integration: marks tests as integration (require live Databricks cluster)")
