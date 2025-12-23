from django.db import connections, models
from django.db.models import signals
from django.dispatch import receiver

from django_pgviews import view


class SchemaObservation(models.Model):
    date = models.DateField()
    temperature = models.IntegerField()


VIEW_SQL = """
WITH summary AS (
    SELECT
        date_trunc('month', date) AS date,
        count(*)
    FROM schemadbtest_schemaobservation
    GROUP BY 1
    ORDER BY date
) SELECT
    ROW_NUMBER() OVER () AS id,
    date,
    count
FROM summary;
"""


class SchemaMonthlyObservationView(view.View):
    sql = VIEW_SQL
    date = models.DateField()
    count = models.IntegerField()

    class Meta:
        managed = False


class SchemaMonthlyObservationMaterializedView(view.MaterializedView):
    sql = VIEW_SQL
    date = models.DateField()
    count = models.IntegerField()

    class Meta:
        managed = False
        indexes = [models.Index(fields=["date"])]


@receiver(signals.pre_migrate)
def create_test_schema(sender, app_config, using, **kwargs):
    command = "CREATE SCHEMA IF NOT EXISTS {};".format("other")
    with connections[using].cursor() as cursor:
        cursor.execute(command)
