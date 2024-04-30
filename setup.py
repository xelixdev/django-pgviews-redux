from pathlib import Path

from setuptools import find_packages, setup

if Path("README.md").exists():
    LONG_DESCRIPTION = Path("README.md").read_text()
else:
    LONG_DESCRIPTION = ""


setup(
    name="django-pgviews-redux",
    version="0.9.4",
    description="Create and manage Postgres SQL Views in Django",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    author="Mikuláš Poul",
    author_email="mikulas.poul@xelix.com",
    license="Public Domain",
    packages=find_packages(),
    url="https://github.com/xelixdev/django-pgviews-redux",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Framework :: Django",
        "Framework :: Django :: 4.2",
        "Framework :: Django :: 5.0",
    ],
)
