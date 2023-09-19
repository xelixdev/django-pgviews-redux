from __future__ import absolute_import, print_function, unicode_literals

from os.path import isfile

from setuptools import setup, find_packages


if isfile("README.md"):
    LONG_DESCRIPTION = open("README.md").read()
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
        "Framework :: Django",
        "Framework :: Django :: 3.2",
        "Framework :: Django :: 4.1",
        "Framework :: Django :: 4.2",
    ],
)
