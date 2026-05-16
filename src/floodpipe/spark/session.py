"""Sedona-enabled SparkSession factory.

Dataproc Serverless supplies the Spark 3.5 runtime; the Sedona and
GeoTools/GDAL jars are attached at submit time (see README Phase 2 for
the ``gcloud dataproc batches submit`` invocation). This module only
wires the Sedona SQL extensions and the Kryo serializer onto whatever
session the runtime hands back — it deliberately pins no jar versions,
since those belong with the submit command, not the application code.

Run locally against a tiny fixture with a pip-installed
``apache-sedona[spark]``; the same factory works there unchanged.
"""

from __future__ import annotations

from sedona.spark import SedonaContext

# Sedona needs Kryo for its geometry/raster serializers; without these
# two settings spatial joins fall back to (slow) Java serialization and
# raster columns fail to ship between executors.
_SEDONA_BASE_CONF: dict[str, str] = {
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.kryo.registrator": "org.apache.sedona.core.serde.SedonaKryoRegistrator",
}


def sedona_session(
    app_name: str = "floodpipe-silver",
    extra_conf: dict[str, str] | None = None,
):
    """Build (or attach to) a Sedona-enabled SparkSession.

    extra_conf is merged last so a caller — or a Dataproc batch property —
    can override anything here. Returns the Sedona-wrapped session; call
    its ``.stop()`` when done.
    """
    builder = SedonaContext.builder().appName(app_name)
    for key, value in {**_SEDONA_BASE_CONF, **(extra_conf or {})}.items():
        builder = builder.config(key, value)
    return SedonaContext.create(builder.getOrCreate())
