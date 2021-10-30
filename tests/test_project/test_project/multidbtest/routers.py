class WeatherPinnedRouter:
    """
    A router to control all database operations on models and views in the
    multidbtest application.
    """

    def db_for_read(self, model, **hints):
        """
        Attempts to read multidbtest models go to weather_db.
        """
        if model._meta.app_label == "multidbtest":
            return "weather_db"
        return "default"

    def db_for_write(self, model, **hints):
        """
        Attempts to write multidbtest models go to weather_db.
        """
        if model._meta.app_label == "multidbtest":
            return "weather_db"
        return "default"

    def allow_relation(self, obj1, obj2, **hints):
        """
        Allow relations if a model in the multidbtest app is involved.
        """
        if obj1._meta.app_label == "multidbtest" or obj2._meta.app_label == "multidbtest":
            return True

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """
        Make sure the multidbtest models only appear in the weather_db database.
        """
        if app_label == "multidbtest":
            return db == "weather_db"
        else:
            return db == "default"
