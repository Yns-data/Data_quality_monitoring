
from setuptools import setup, find_packages

setup(
    name="Data_quality_monitoring",
    version="0.1.0",
    author="Younes Essoualhi",
    author_email="younesessoualhi@gmail.com",
    description="Une courte description de mon package",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "numpy"
    ],
    python_requires=">=3.8",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
