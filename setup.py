from setuptools import setup, find_packages

setup(
    name="fraud-detection",
    version="0.1",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "apache-flink==1.20.0",
    ],
    python_requires=">=3.7",
)