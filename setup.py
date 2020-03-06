from __future__ import absolute_import, print_function, unicode_literals

from os.path import isfile

from setuptools import setup, find_packages


if isfile("README.md"):
    LONG_DESCRIPTION = open("README.md").read()
else:
    LONG_DESCRIPTION = ""


setup(
    name="django-pgviews-redux",
    version="0.6.0",
    description="Create and manage Postgres SQL Views in Django",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    author="Mikuláš Poul",
    author_email="mikulaspoul@gmail.com",
    license="Public Domain",
    packages=find_packages(),
    url="https://github.com/mikicz/django-pgviews",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Framework :: Django",
        "Framework :: Django :: 2.0",
        "Framework :: Django :: 3.0",
    ],
)
