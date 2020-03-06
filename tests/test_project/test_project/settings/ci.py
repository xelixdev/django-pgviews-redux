import os

from .base import *


DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': os.environ.get('DB_NAME', 'circle_test'),
        'USER': os.environ.get('DB_USER', 'ubuntu'),
        'PASSWORD': os.environ.get('DB_PASSWORD', ':'),
        'HOST': 'localhost',
        'PORT': '5432',
    },
}
