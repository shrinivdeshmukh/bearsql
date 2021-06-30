"""Top-level package for bearsql."""

__author__ = """Shrinivas Vijay Deshmukh"""
__email__ = 'shrinivas.deshmukh11@gmail.com'
__version__ = '0.1.0'

from bearsql.log_source import Logging
from os import getenv

logger = Logging(log_level=getenv('LOG_LEVEL', 'ERROR')).get_logger()

from bearsql.bearsql import SqlContext as SqlContext