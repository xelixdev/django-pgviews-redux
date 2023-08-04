from datetime import timedelta

from django.db import models, connections
from django.db.models import signals
from django.dispatch import receiver
from django.utils import timezone

from django_pgviews import view


class TestModel(models.Model):
    """
    Test model with some basic data for running migrate tests against.
    """

    name = models.CharField(max_length=100)


class Superusers(view.View):
    projection = ["auth.User.*"]
    sql = """SELECT * FROM auth_user WHERE is_superuser = TRUE;"""


class LatestSuperusers(view.View):  # concept doesn't make much sense, but it will get the job done
    projection = ["auth.User.*"]

    @classmethod
    def get_sql(cls):
        return view.ViewSQL(
            """SELECT * FROM auth_user WHERE is_superuser = TRUE and date_joined >= %s;""",
            [timezone.now() - timedelta(days=5)],
        )


class SimpleUser(view.View):
    projection = ["auth.User.username", "auth.User.password"]
    # The row_number() window function is needed so that Django sees some kind
    # of 'id' field. We could also grab the one from `auth.User`, but this
    # seemed like more fun :)
    sql = """SELECT username, password, row_number() OVER () AS id FROM auth_user;"""


class RelatedView(view.ReadOnlyView):
    sql = """SELECT id AS model_id, id FROM viewtest_testmodel"""
    model = models.ForeignKey(TestModel, on_delete=models.CASCADE)


class MaterializedRelatedView(view.ReadOnlyMaterializedView):
    sql = """SELECT id AS model_id, id FROM viewtest_testmodel"""
    model = models.ForeignKey(TestModel, on_delete=models.DO_NOTHING)


class DependantView(view.ReadOnlyView):
    dependencies = ("viewtest.RelatedView",)
    sql = """SELECT model_id from viewtest_relatedview;"""


class DependantMaterializedView(view.ReadOnlyMaterializedView):
    dependencies = ("viewtest.MaterializedRelatedView",)
    sql = """SELECT model_id from viewtest_materializedrelatedview;"""


class MaterializedRelatedViewWithIndex(view.ReadOnlyMaterializedView):
    concurrent_index = "id"
    sql = """SELECT id AS model_id, id FROM viewtest_testmodel"""
    model = models.ForeignKey(TestModel, on_delete=models.DO_NOTHING)

    class Meta:
        managed = False
        indexes = [models.Index(fields=["model"])]


class CustomSchemaMaterializedRelatedViewWithIndex(view.ReadOnlyMaterializedView):
    concurrent_index = "id"
    sql = """SELECT id AS model_id, id FROM viewtest_testmodel"""
    model = models.ForeignKey(TestModel, on_delete=models.DO_NOTHING)

    class Meta:
        db_table = "test_schema.my_custom_view_with_index"
        managed = False
        indexes = [models.Index(fields=["model"])]


class CustomSchemaView(view.ReadOnlyView):
    sql = """SELECT id AS model_id, id FROM viewtest_testmodel"""
    model = models.ForeignKey(TestModel, on_delete=models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = "test_schema.my_custom_view"


class MaterializedRelatedViewWithNoData(view.ReadOnlyMaterializedView):
    sql = """SELECT id AS model_id, id FROM viewtest_testmodel"""
    model = models.ForeignKey(TestModel, on_delete=models.DO_NOTHING)
    with_data = False


class MaterializedRelatedViewWithReallyReallyReallyReallyReallyReallyLongName(view.ReadOnlyMaterializedView):
    sql = """SELECT id AS model_id, id FROM viewtest_testmodel"""
    model = models.ForeignKey(TestModel, on_delete=models.DO_NOTHING)


@receiver(signals.post_migrate)
def create_test_schema(sender, app_config, using, **kwargs):
    command = "CREATE SCHEMA IF NOT EXISTS {};".format("test_schema")
    with connections[using].cursor() as cursor:
        cursor.execute(command)
