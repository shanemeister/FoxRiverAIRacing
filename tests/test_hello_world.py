import pytest
from hello_world import hello

def test_hello_world():
    assert hello() == "Hello, World!"