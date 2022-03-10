package com.lidl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class SparkChallenge {

	public static void main(String[] args) {
		// Please paste the path appropriately upto the winutils.exe folder to set "hadoop.home.dir" when running locally.
		System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-3.3.1\\");
		SparkSession spark = SparkSession.builder()
				.master("local[*]")
				//Please comment the above line when running on cluster.
				.getOrCreate();

		SparkChallenge sparkChallenge = new SparkChallenge();
		Dataset<Row> source = sparkChallenge.readCsv(spark, "src/test/resources/events.csv");

		Dataset<Row> baseDs = sparkChallenge.getInterimBaseRecords(spark, source);
		sparkChallenge.writeData(baseDs, "src/test/output/base_data/");

		Dataset<Row> output = sparkChallenge.getOneRowForTenMinutes(spark, baseDs);
		sparkChallenge.writeData(output, "src/test/output/one_record_per_10_mins/");

		spark.stop();
	}
	/***
	 * Read the csv file. i.e. events.csv
	 * @param spark
	 * @param path
	 * @return
	 */
	public Dataset<Row> readCsv(SparkSession spark, String path) {
		Dataset<Row> source = spark.read().option("header", true).csv(path);
		return source;
	}

	/***
	 * Get one record for each 10 minutes.
	 * @param spark
	 * @param src
	 * @return
	 */
	public Dataset<Row> getOneRowForTenMinutes(SparkSession spark, Dataset<Row> src) {

		src.createOrReplaceTempView("interim");

		Dataset<Row> output = spark
				.sql("SELECT b.timestamp, b.open_val as actions_count_open, b.close_val as actions_count_close from "
						+ "(SELECT a.row_num, a.timestamp, a.actions_count_open, "
						+ "sum(a.actions_count_open) over(order by row_num rows between current row and 9 following) as open_val, "
						+ "a.actions_count_close, "
						+ "sum(a.actions_count_close) over(order by row_num rows between current row and 9 following) as close_val,"
						+ "MOD(a.row_num, 10) as mod_val from "
						+ "(SELECT timestamp, actions_count_open, "
						+ "actions_count_close, row_number() over(order by timestamp) as row_num FROM interim) "
						+ "a) b where b.mod_val = 1");

		spark.catalog().dropTempView("interim");

		return output;
	}
	
	/***
	 * Form a base record for future computations.
	 * @param spark
	 * @param src
	 * @return
	 */
	public Dataset<Row> getInterimBaseRecords(SparkSession spark, Dataset<Row> src) {

		String format = "yyyyMMddhhmm";
		src = src.withColumn("timestamp_new", functions.date_format(functions.col("time"), format));
		src.createOrReplaceTempView("src");

		Dataset<Row> open_action = spark
				.sql("SELECT timestamp_new AS timestamp_new_open, count(*) AS actions_count_open FROM src "
						+ "WHERE lower(action) = 'open' GROUP BY timestamp_new ORDER BY timestamp_new");

		Dataset<Row> close_action = spark
				.sql("SELECT timestamp_new AS timestamp_new_close, count(*) AS actions_count_close FROM src "
						+ "WHERE lower(action) = 'close' GROUP BY timestamp_new ORDER BY timestamp_new");

		Dataset<Row> joined = open_action.join(close_action,
				open_action.col("timestamp_new_open").equalTo(close_action.col("timestamp_new_close")), "full_outer");
		
		joined.createOrReplaceTempView("joined");
		
		Dataset<Row> interim = spark.sql("SELECT COALESCE(timestamp_new_open, timestamp_new_close) AS timestamp, "
				+ "COALESCE(actions_count_open,0) AS actions_count_open, "
				+ "COALESCE(actions_count_close,0) AS actions_count_close FROM joined");

		spark.catalog().dropTempView("src");
		spark.catalog().dropTempView("joined");
		return interim;
	}
	
	/***
	 * Save the data in parquet format.
	 * @param output
	 * @param path
	 */
	public void writeData(Dataset<Row> output, String path) {
		
		Long counts = output.count();
		if (counts <= 100000)
			output.repartition(2).write().mode(SaveMode.Overwrite).format("parquet").save(path);
		else if (counts >= 1000000)
			output.repartition(5).write().mode(SaveMode.Overwrite).format("parquet").save(path);
	}
}
