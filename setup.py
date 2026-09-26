from setuptools import find_packages, setup

setup(
    name="night-market-foundation",
    version="0.1.0",
    description="中医文化夜市协作基础层",
    package_dir={"": "src"},
    packages=find_packages("src"),
    python_requires=">=3.11",
)
