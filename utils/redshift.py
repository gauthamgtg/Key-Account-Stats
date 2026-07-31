"""Shared Redshift connection decorator for scripts and tools."""

from functools import wraps

import psycopg2


def redshift_connection(dbname, user, password, host, port):
    """Inject connection and cursor as kwargs; closes them after the wrapped call."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                connection = psycopg2.connect(
                    dbname=dbname,
                    user=user,
                    password=password,
                    host=host,
                    port=port,
                )
                cursor = connection.cursor()

                print("Connected to Redshift!")

                result = func(*args, connection=connection, cursor=cursor, **kwargs)

                cursor.close()
                connection.close()

                print("Disconnected from Redshift!")

                return result

            except Exception as e:
                raise RuntimeError(f"Redshift operation failed: {e}") from e

        return wrapper

    return decorator
