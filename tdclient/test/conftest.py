import pytest

try:
    from urllib3.contrib.pyopenssl import (
        extract_from_urllib3,
        inject_into_urllib3,
    )
except ImportError:
    inject_into_urllib3 = lambda: None  # noqa: E731
    extract_from_urllib3 = lambda: None  # noqa: E731


@pytest.fixture(autouse=True)
def pyopenssl_inject_into_urllib3():
    inject_into_urllib3()
    try:
        yield
    finally:
        extract_from_urllib3()
