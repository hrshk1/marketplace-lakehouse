# AWS mapping

This project runs entirely on open-source components on one machine. It has not been deployed on
AWS, and nothing in this repository has been tested against an AWS service. This page maps each
local component to the AWS service that plays the same role and lists what a migration would
actually involve.

## Component mapping

| Local component | AWS equivalent | What stays the same | What differs |
| --- | --- | --- | --- |
| MinIO buckets `lake`, `warehouse-exports` | Amazon S3 | The S3 API; Iceberg file layout | No endpoint override or path-style access; IAM roles instead of static keys |
| PySpark in local mode | EMR (on EC2 or Serverless) or AWS Glue jobs | The PySpark and Spark SQL code | Cluster sizing, job submission, real shuffle and executors |
| Iceberg JDBC catalog on PostgreSQL | AWS Glue Data Catalog | Iceberg tables and namespaces | Catalog implementation and its configuration |
| S3A file system (`hadoop-aws`) | Iceberg `S3FileIO` (or EMRFS) | Nothing visible to the code | `s3://` paths instead of `s3a://` |
| PostgreSQL schema `dw` | Amazon Redshift | Star schema, analytics SQL (mostly), staging-then-swap load | `COPY` from S3 with an IAM role, distribution and sort keys, no ordinary indexes |
| Airflow 3 in Docker | Amazon MWAA | DAG files and the `marketplace` package | Packaging as `requirements.txt` and plugins; tasks submit jobs instead of running Spark in-process |
| Metabase | Amazon QuickSight, or Metabase on ECS | Dashboards and their SQL | QuickSight would need the dashboards rebuilt |
| PostgreSQL `oltp`, MongoDB | Amazon RDS for PostgreSQL, Amazon DocumentDB | Extraction logic | Network access, secrets, read replicas for extraction |
| `.env` file | AWS Secrets Manager or SSM Parameter Store | Setting names | How values are injected |

## What a migration would change

1. **Storage and catalog configuration.** Replace the JDBC catalog settings in `src/spark.py`
   with `org.apache.iceberg.aws.glue.GlueCatalog` and `S3FileIO`, change the warehouse path to
   `s3://`, and drop the S3A endpoint, path-style and static-credential settings. The jar set
   changes with it: the Iceberg AWS bundle replaces `hadoop-aws` and the SDK v1 bundle. On EMR the
   matching Iceberg runtime ships with the release and would be used instead of the pinned jar.

2. **Where Spark runs.** Today each Airflow task imports the package and runs Spark inside the
   task process. On AWS a task should only submit and watch a job: an EMR step, an EMR Serverless
   job run or a Glue job. `src/orchestration.py` would be replaced by operators from the Amazon
   provider, and each pipeline step would need a small job entry point. The transformation code
   does not change.

3. **Sizing and partitioning that local mode hides.** Shuffle partitions, executor memory and
   JDBC read parallelism are all set for one JVM and small data. Extraction of large tables would
   need `partitionColumn` and bounds; the fact rebuild would need attention to skew on popular
   products and customers.

4. **Warehouse load.** `src/publish/load.py` streams CSV through a PostgreSQL connection.
   Redshift loads straight from S3: `COPY ... FROM 's3://...' IAM_ROLE ... FORMAT AS PARQUET`
   into a staging table, then a transactional swap. The type mapping would be rewritten for
   Redshift types, indexes replaced by sort and distribution keys, and the full reload replaced
   by a load per order month. Alternatively, Redshift Spectrum or Athena could query the Iceberg
   tables in place and remove the copy.

5. **Analytics SQL.** The queries use PostgreSQL features. `FILTER (WHERE ...)`,
   `percentile_cont ... WITHIN GROUP` and `age()` need checking or rewriting for Redshift.

6. **Identity and secrets.** Static keys in `.env` become IAM roles for the jobs and MWAA, and
   database credentials move to Secrets Manager. Buckets need encryption and access policies,
   and the sources need network paths from the processing environment.

7. **Operations.** Logs go to CloudWatch. Failure records in `ops.pipeline_runs` should also
   raise an alert (SNS or a chat webhook), which the local stack does not do. Iceberg maintenance
   stays necessary: the weekly DAG's procedures run unchanged on EMR, or the Glue Data Catalog's
   managed compaction can take over.

8. **Cost controls that do not exist locally.** S3 lifecycle rules for the export bucket and for
   expired snapshots, job timeouts, and right-sizing of clusters become part of the design.

## What is deliberately not claimed

- No performance numbers in this repository say anything about AWS. They were measured on one
  laptop with Spark in local mode.
- The mapping is by role. Behavioural differences, such as Glue Catalog commit semantics, S3
  request limits and Redshift concurrency, would have to be tested, not assumed.
