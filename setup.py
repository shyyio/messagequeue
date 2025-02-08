from setuptools import setup

setup(
    name="messagequeue",
    version="1.0",
    packages=["messagequeue"],
    install_requires=[
        "redis", "orjson"
    ]
)
