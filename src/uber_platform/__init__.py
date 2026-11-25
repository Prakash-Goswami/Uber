"""
Uber Lakehouse Platform on Databricks.

End-to-end reusable package for public dataset ingestion, real-time event simulation,
H3 geospatial indexing, Medallion Bronze/Silver/Gold transformations, SCD Type 2,
and Michelangelo-inspired PySpark ML pipelines.
"""

from uber_platform.config import LakehouseConfig, bootstrap_lakehouse

__all__ = ["LakehouseConfig", "bootstrap_lakehouse"]
__version__ = "1.0.0"
