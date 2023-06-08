from setuptools import setup, find_packages

setup(
    name="man_etl",
    version="0.0.4",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "Click",
        "arcticdb",
        "pandas",
    ],
    entry_points="""
        [console_scripts]
        csv_to_arctic=WM9L8_IMA.etl_pipelines.csv_to_arctic:csv_to_arctic
    """,
)