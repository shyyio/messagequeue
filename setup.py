from setuptools import setup

setup(
    name="messagequeue",
    version="2.2",
    packages=["messagequeue"],
    install_requires=[
        "redis", "orjson"
    ]
)
