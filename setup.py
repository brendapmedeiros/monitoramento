from setuptools import setup, find_packages

setup(
    name="monitoramento",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "pyyaml",
        "great-expectations",
        "requests",
    ],
    python_requires=">=3.8",
)