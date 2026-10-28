"""Read real HDFS data/models for live command-line evidence after the DAG."""
import json
import sys
from stages import spark_session, raw_csv, HDFS, OUT, META

mode = sys.argv[1]
spark = spark_session('terminal-' + mode)
try:
    from pyspark.sql import functions as F
    if mode == 'read':
        frame = raw_csv(spark)
        print(f'CSV rows={frame.count()} | source columns={len(frame.columns) - 1}', flush=True)
        selected = frame.select('collision_id', 'crash_date_time', 'city_name', 'number_injured', 'number_killed')
        selected.printSchema()
        selected.show(5, truncate=24)
    elif mode == 'clean':
        frame = spark.read.parquet(HDFS + '/processed/cleaned')
        frame.select('collision_id', 'crash_timestamp', 'number_injured', 'number_killed', 'casualties_complete', 'month').printSchema()
        frame.agg(F.count('*').alias('retained_rows'),
                  F.sum(F.col('number_injured').isNull().cast('int')).alias('unknown_injured'),
                  F.sum(F.col('number_killed').isNull().cast('int')).alias('unknown_killed')).show(truncate=False)
        frame.groupBy('month').count().orderBy('month').show(12, truncate=False)
    elif mode == 'sql':
        spark.read.parquet(HDFS + '/processed/cleaned').createOrReplaceTempView('crashes')
        query = '''SELECT month, COUNT(*) AS crashes,
 SUM(number_injured) AS observed_injured, COUNT(number_injured) AS injured_known,
 SUM(number_killed) AS observed_killed, COUNT(number_killed) AS killed_known
FROM crashes GROUP BY month ORDER BY month'''
        print('Executing Spark SQL against HDFS Parquet:\n' + query, flush=True)
        spark.sql(query).show(12, truncate=False)
    elif mode == 'rfm':
        from pyspark.ml.clustering import KMeansModel
        from pyspark.ml.feature import StandardScalerModel
        model = KMeansModel.load(HDFS + '/output/models/kmeans')
        scaler = StandardScalerModel.load(HDFS + '/output/models/standard_scaler')
        print(f'Loaded real HDFS models: KMeans k={model.getK()}, StandardScaler dimension={len(scaler.mean)}', flush=True)
        print('M = observed casualty magnitude, NOT monetary value.', flush=True)
        frame = spark.read.parquet(HDFS + '/output/rfm_clusters')
        frame.groupBy('eligible').count().orderBy('eligible').show(truncate=False)
        frame.filter('eligible').groupBy('cluster').agg(F.count('*').alias('geographic_groups'), F.sum('f').alias('crashes'), F.sum('m_observed').alias('observed_casualties')).orderBy('cluster').show(truncate=False)
        frame.filter('eligible').select('city_name', 'r', 'f', 'm_observed', 'casualty_coverage', 'cluster').orderBy(F.desc('f'), 'city_name').show(5, truncate=24)
    elif mode == 'stream':
        info = json.loads((OUT / 'data-quality/stream-reconciliation.json').read_text())
        print('Reading actual checkpointed Structured Streaming output: ' + info['hdfs_output'], flush=True)
        frame = spark.read.parquet(info['hdfs_output'])
        frame.agg(F.count('*').alias('persisted_rows'), F.countDistinct('event_id').alias('unique_event_ids')).show(truncate=False)
        frame.groupBy('county_code', 'city_id').count().orderBy(F.desc('count'), 'county_code', 'city_id').show(5, truncate=False)
    else:
        raise SystemExit('Unknown terminal check mode: ' + mode)
finally:
    spark.stop()
