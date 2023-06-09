from setuptools import setup, find_packages

setup(
    name="man_etl",
    version="0.0.6",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "Click",
        "arcticdb",
        "pandas",
    ],
    entry_points="""
        [console_scripts]
        csv_to_arctic=man_etl.etl_pipelines.csv_to_arctic:csv_to_arctic
        ons_to_arctic=man_etl.etl_pipelines.ons_to_arctic:ons_to_arctic
        arctic_transform=man_etl.etl_pipelines.arctic_transform:arctic_transform
        yf_to_arctic=man_etl.etl_pipelines.yf_to_arctic:yf_to_arctic
    """,
)
