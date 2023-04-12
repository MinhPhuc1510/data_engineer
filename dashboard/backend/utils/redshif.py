import logging
import redshift_connector

_logger = logging.getLogger(__name__)

class RedshiftSingleton(type):
    """
    The Redshif Singleton metaclass.
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class Redshift():
    """
    The Stackstorm Provider.
    """

    def __init__(self):
        self.host = ''
        self.database = ''
        self.user = ''
        self.password = ''

        #Connect to the cluster
        self.conn = redshift_connector.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password
        )

