import pytest

try:
    from urllib3.contrib.pyopenssl import inject_into_urllib3, extract_from_urllib3
except ImportError:
    inject_into_urllib3 = lambda: None
    extract_from_urllib3 = lambda: None


@pytest.fixture(autouse=True)
def pyopenssl_inject_into_urllib3():
    inject_into_urllib3()
    try:
        yield
    finally:
        extract_from_urllib3()
