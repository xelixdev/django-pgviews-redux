__all__ = ["ProgrammingError"]

try:
    from psycopg import ProgrammingError
except ImportError:
    from psycopg2 import ProgrammingError
