# SQL Views for Postgres

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Adds first-class support for [PostgreSQL Views][pg-views] in the Django ORM.
Fork of the original [django-pgviews][django-pgviews] by [mypebble][mypebble] with support for Django 3.2+.

[pg-views]: http://www.postgresql.org/docs/9.1/static/sql-createview.html
[django-pgviews]: https://github.com/mypebble/django-pgviews
[mypebble]: https://github.com/mypebble

## Installation

Install via pip:

    pip install django-pgviews-redux

Add to installed applications in settings.py:

```python
INSTALLED_APPS = (
  # ...
  'django_pgviews',
)
```

## Examples

```python
from django.db import models

from django_pgviews import view as pg


class Customer(models.Model):
    name = models.CharField(max_length=100)
    post_code = models.CharField(max_length=20)
    is_preferred = models.BooleanField(default=False)

    class Meta:
        app_label = 'myapp'

class PreferredCustomer(pg.View):
    projection = ['myapp.Customer.*',]
    dependencies = ['myapp.OtherView',]
    sql = """SELECT * FROM myapp_customer WHERE is_preferred = TRUE;"""

    class Meta:
      app_label = 'myapp'
      db_table = 'myapp_preferredcustomer'
      managed = False
```

**NOTE** It is important that we include the `managed = False` in the `Meta` so
Django 1.7 migrations don't attempt to create DB tables for this view.

The SQL produced by this might look like:

```postgresql
CREATE VIEW myapp_preferredcustomer AS
SELECT * FROM myapp_customer WHERE is_preferred = TRUE;
```

To create all your views, run ``python manage.py sync_pgviews``

You can also specify field names, which will map onto fields in your View:

```python
from django_pgviews import view as pg


VIEW_SQL = """
    SELECT name, post_code FROM myapp_customer WHERE is_preferred = TRUE
"""


class PreferredCustomer(pg.View):
    name = models.CharField(max_length=100)
    post_code = models.CharField(max_length=20)

    sql = VIEW_SQL
```

## Usage

To map onto a View, simply extend `pg_views.view.View`, assign SQL to the
`sql` argument and define a `db_table`. You must _always_ set `managed = False`
on the `Meta` class.

Views can be created in a number of ways:

1. Define fields to map onto the VIEW output
2. Define a projection that describes the VIEW fields

### Define Fields

Define the fields as you would with any Django Model:

```python
from django_pgviews import view as pg


VIEW_SQL = """
    SELECT name, post_code FROM myapp_customer WHERE is_preferred = TRUE
"""


class PreferredCustomer(pg.View):
    name = models.CharField(max_length=100)
    post_code = models.CharField(max_length=20)

    sql = VIEW_SQL

    class Meta:
      managed = False
      db_table = 'my_sql_view'
```

### Define Projection

`django-pgviews` can take a projection to figure out what fields it needs to
map onto for a view. To use this, set the `projection` attribute:

```python
from django_pgviews import view as pg


class PreferredCustomer(pg.View):
    projection = ['myapp.Customer.*',]
    sql = """SELECT * FROM myapp_customer WHERE is_preferred = TRUE;"""

    class Meta:
      db_table = 'my_sql_view'
      managed = False
```

This will take all fields on `myapp.Customer` and apply them to
`PreferredCustomer`

## Features

### Updating Views

Sometimes your models change and you need your Database Views to reflect the new
data. Updating the View logic is as simple as modifying the underlying SQL and
running:

```
python manage.py sync_pgviews --force
```

This will forcibly update any views that conflict with your new SQL.

### Dependencies

You can specify other views you depend on. This ensures the other views are
installed beforehand. Using dependencies also ensures that your views get
refreshed correctly when using `sync_pgviews --force`.

**Note:** Views are synced after the Django application has migrated and adding
models to the dependency list will cause syncing to fail.

Example:

```python
from django_pgviews import view as pg

class PreferredCustomer(pg.View):
    dependencies = ['myapp.OtherView',]
    sql = """SELECT * FROM myapp_customer WHERE is_preferred = TRUE;"""

    class Meta:
      app_label = 'myapp'
      db_table = 'myapp_preferredcustomer'
      managed = False
```

### Materialized Views

Postgres 9.3 and up supports [materialized views](http://www.postgresql.org/docs/current/static/sql-creatematerializedview.html)
which allow you to cache the results of views, potentially allowing them
to load faster.

However, you do need to manually refresh the view. To do this automatically,
you can attach [signals](https://docs.djangoproject.com/en/1.8/ref/signals/)
and call the refresh function.

Example:

```python
from django_pgviews import view as pg


VIEW_SQL = """
    SELECT name, post_code FROM myapp_customer WHERE is_preferred = TRUE
"""

class Customer(models.Model):
    name = models.CharField(max_length=100)
    post_code = models.CharField(max_length=20)
    is_preferred = models.BooleanField(default=True)


class PreferredCustomer(pg.MaterializedView):
    name = models.CharField(max_length=100)
    post_code = models.CharField(max_length=20)

    sql = VIEW_SQL


@receiver(post_save, sender=Customer)
def customer_saved(sender, action=None, instance=None, **kwargs):
    PreferredCustomer.refresh()
```

#### Concurrent refresh

Postgres 9.4 and up allow materialized views to be refreshed concurrently, without blocking reads, as long as a
unique index exists on the materialized view. To enable concurrent refresh, specify the name of a column that can be
used as a unique index on the materialized view. Unique index can be defined on more than one column of a materialized 
view. Once enabled, passing `concurrently=True` to the model's refresh method will result in postgres performing the 
refresh concurrently. (Note that the refresh method itself blocks until the refresh is complete; concurrent refresh is 
most useful when materialized views are updated in another process or thread.)

Example:

```python
from django_pgviews import view as pg


VIEW_SQL = """
    SELECT id, name, post_code FROM myapp_customer WHERE is_preferred = TRUE
"""

class PreferredCustomer(pg.MaterializedView):
    concurrent_index = 'id, post_code'
    sql = VIEW_SQL

    name = models.CharField(max_length=100)
    post_code = models.CharField(max_length=20)


@receiver(post_save, sender=Customer)
def customer_saved(sender, action=None, instance=None, **kwargs):
    PreferredCustomer.refresh(concurrently=True)
```

#### Indexes

As the materialized view isn't defined through the usual Django model fields, any indexes defined there won't be 
created on the materialized view. Luckily Django provides a Meta option called `indexes` which can be used to add custom
indexes to models. `pg_views` supports defining indexes on materialized views using this option.

In the following example, one index will be created, on the `name` column. The `db_index=True` on the field definition
for `post_code` will get ignored.

```python
from django_pgviews import view as pg


VIEW_SQL = """
    SELECT id, name, post_code FROM myapp_customer WHERE is_preferred = TRUE
"""

class PreferredCustomer(pg.MaterializedView):
    sql = VIEW_SQL

    name = models.CharField(max_length=100)
    post_code = models.CharField(max_length=20, db_index=True)
    
    class Meta:
        managed = False  # don't forget this, otherwise Django will think it's a regular model
        indexes = [
             models.Index(fields=["name"]),
        ]
```

#### WITH NO DATA

Materialized views can be created either with or without data. By default, they are created with data, however
`pg_views` supports creating materialized views without data, by defining `with_data = False` for the
`pg.MaterializedView` class. Such views then do not support querying until the first 
refresh (raising `django.db.utils.OperationalError`).

Example:

```python
from django_pgviews import view as pg

class PreferredCustomer(pg.MaterializedView):
    concurrent_index = 'id, post_code'
    sql = """
        SELECT id, name, post_code FROM myapp_customer WHERE is_preferred = TRUE
    """
    with_data = False

    name = models.CharField(max_length=100)
    post_code = models.CharField(max_length=20)
```

#### Conditional materialized views recreate

Since all materialized views are recreated on running `migrate`, it can lead to obsolete recreations even if there
were no changes to the definition of the view. To prevent this, version 0.7.0 and higher contain a feature which
checks existing materialized view definition in the database (if the mat. view exists at all) and compares the
definition with the one currently defined in your `pg.MaterializedView` subclass. If the definition matches
exactly, the re-create of materialized view is skipped.

This feature is enabled by setting the `MATERIALIZED_VIEWS_CHECK_SQL_CHANGED` in your Django settings to `True`, 
which enables the feature when running `migrate`. The command `sync_pgviews` uses this setting as well,
however it also has switches `--enable-materialized-views-check-sql-changed` and
`--disable-materialized-views-check-sql-changed` which override this setting for that command.

This feature also takes into account indexes. When a view is deemed not needing recreating, the process will still
check the indexes on the table and delete any extra indexes and create any missing indexes. This reconciliation
is done through the index name, so if you use custom names for your indexes, it might happen that it won't get updated
on change of the content but not the name.

### Custom Schema

You can define any table name you wish for your views. They can even live inside your own custom
[PostgreSQL schema](http://www.postgresql.org/docs/current/static/ddl-schemas.html).

```python
from django_pgviews import view as pg


class PreferredCustomer(pg.View):
    sql = """SELECT * FROM myapp_customer WHERE is_preferred = TRUE;"""

    class Meta:
      db_table = 'my_custom_schema.preferredcustomer'
      managed = False
```

### Dynamic View SQL

If you need a dynamic view SQL (for example if it needs a value from settings in it), you can override the `run_sql`
classmethod on the view to return the SQL. The method should return a namedtuple `ViewSQL`, which contains the query
and potentially the params to `cursor.execute` call. Params should be either None or a list of parameters for the query.

```python
from django.conf import settings
from django_pgviews import view as pg


class PreferredCustomer(pg.View):
    @classmethod
    def get_sql(cls):
        return pg.ViewSQL(
            """SELECT * FROM myapp_customer WHERE is_preferred = TRUE and created_at >= %s;""",
            [settings.MIN_PREFERRED_CUSTOMER_CREATED_AT]
        )

    class Meta:
      db_table = 'preferredcustomer'
      managed = False
```

### Sync Listeners

django-pgviews 0.5.0 adds the ability to listen to when a `post_sync` event has
occurred.

#### `view_synced`

Fired every time a VIEW is synchronised with the database.

Provides args:
* `sender` - View Class
* `update` - Whether the view to be updated
* `force` - Whether `force` was passed
* `status` - The result of creating the view e.g. `EXISTS`, `FORCE_REQUIRED`
* `has_changed` - Whether the view had to change

#### `all_views_synced`

Sent after all Postgres VIEWs are synchronised.

Provides args:
* `sender` - Always `None`


### Multiple databases

django-pgviews can use multiple databases.  Similar to Django's `migrate`
management command, our commands (`clear_pgviews`, `refresh_pgviews`,
`sync_pgviews`) operate on one database at a time. You can specify which
database to synchronize by providing the `--database` option. For example:

```shell
python manage.py sync_pgviews  # uses default db
python manage.py sync_pgviews --database=myotherdb
```

Unless using custom routers, django-pgviews will sync all views to the specified
database. If you want to interact with multiple databases automatically, you'll
need to take some additional steps. Please refer to Django's [Automatic database
routing](https://docs.djangoproject.com/en/3.2/topics/db/multi-db/#automatic-database-routing)
to pin views to specific databases.


## Django Compatibility

<table>
  <thead>
    <tr>
      <th>Django Version</th>
      <th>Django-PGView Version</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>1.4 and down</td>
      <td>Unsupported</td>
    </tr>
    <tr>
      <td>1.5</td>
      <td>0.0.1</td>
    </tr>
    <tr>
      <td>1.6</td>
      <td>0.0.3</td>
    </tr>
    <tr>
      <td>1.7</td>
      <td>0.0.4</td>
    </tr>
    <tr>
      <td>1.9</td>
      <td>0.1.0</td>
    </tr>
    <tr>
      <td>1.10</td>
      <td>0.2.0</td>
    </tr>
    <tr>
      <td>2.2</td>
      <td>0.6.0</td>
    </tr>
    <tr>
      <td>3.0</td>
      <td>0.6.0</td>
    </tr>
    <tr>
      <td>3.1</td>
      <td>0.6.1</td>
    <tr>
      <td>3.2</td>
      <td>0.7.1</td>
    </tr>
    <tr>
      <td>4.0</td>
      <td>0.8.1</td>
    </tr>
    <tr>
      <td>4.1</td>
      <td>0.8.4</td>
    </tr>
  </tbody>
</table>

## Python 3 Support

Django PGViews Redux only officially supports Python 3.7+, it might work on 3.6, but there's no guarantees.
