# One image runs everything that executes project code: the Airflow services
# and the on-demand `tools` container used by scripts/dev.py.

FROM eclipse-temurin:17.0.21_9-jre-jammy AS jre

FROM apache/airflow:3.3.2-python3.11

USER root
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
RUN mkdir -p /opt/project /opt/spark-jars \
    && chown -R airflow:0 /opt/project /opt/spark-jars

ENV JAVA_HOME=/opt/java/openjdk \
    PATH="/opt/java/openjdk/bin:${PATH}" \
    SPARK_JARS_DIR=/opt/spark-jars \
    PROJECT_ROOT=/opt/project \
    PYTHONDONTWRITEBYTECODE=1

USER airflow
WORKDIR /opt/project

COPY --chown=airflow:0 scripts/fetch_jars.py scripts/fetch_jars.py
RUN python scripts/fetch_jars.py --dest "${SPARK_JARS_DIR}"

# Installed in editable mode: at runtime the repository is bind-mounted over
# /opt/project, so code changes take effect without rebuilding the image.
COPY --chown=airflow:0 pyproject.toml pyproject.toml
COPY --chown=airflow:0 src src
RUN pip install --no-cache-dir -e ".[dev]" \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.11.txt" \
    && pip check
