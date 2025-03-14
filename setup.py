from setuptools import setup

setup(
    name="messagequeue",
    version="3.1",
    packages=["messagequeue"],
    install_requires=[
        "redis", "orjson"
    ]
)
