from django.db import models

from django_pgviews import view


class Observation(models.Model):
    date = models.DateField()
    temperature = models.IntegerField()


VIEW_SQL = """
WITH summary AS (
    SELECT
        date_trunc('month', date) AS date,
        count(*)
    FROM multidbtest_observation
    GROUP BY 1
    ORDER BY date
) SELECT
    ROW_NUMBER() OVER () AS id,
    date,
    count
FROM summary;
"""


class MonthlyObservation(view.ReadOnlyMaterializedView):
    sql = VIEW_SQL
    date = models.DateField()
    count = models.IntegerField()

    class Meta:
        managed = False
