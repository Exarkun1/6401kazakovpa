from setuptools import setup

setup(
    name="time_series",
    version="1.0",
    description="Time series analysing module.",
    author="Kazakov Pavel",
    author_email="propcool@gmail.com",
    packages=["time_series"],
    install_requires=[
        "numpy",
        "pandas"
    ],
)