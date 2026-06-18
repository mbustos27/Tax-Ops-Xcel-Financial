"""WSGI-1 / GitHub #137 — production WSGI dependencies."""


def test_waitress_package_available() -> None:
    import importlib.util

    assert importlib.util.find_spec("waitress") is not None
