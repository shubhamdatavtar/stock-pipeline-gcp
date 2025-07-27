from setuptools import setup
setup(
    name="stock-dataflow",
    version="0.1",
    packages=[],
    install_requires=[
        "google-cloud-pubsub==2.*",
        "google-cloud-storage==2.*",
        "google-cloud-bigquery==3.*",
        "google-cloud-dataflow==0.8.*"
    ]
)