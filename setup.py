from setuptools import setup

setup(
    name="messagequeue",
    version="3.0",
    packages=["messagequeue"],
    install_requires=[
        "redis", "orjson"
    ]
)
