package com.lidl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class AverageComputationSpark {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// Please paste the path appropriately upto the winutils.exe folder to set "hadoop.home.dir" when running locally.
		System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-3.3.1\\");
		AverageComputationSpark object = new AverageComputationSpark();
		SparkSession spark = SparkSession.builder()
				.master("local[*]")
				//Please comment the above line when running on cluster.
				.getOrCreate();

		Dataset<Row> source = spark.read().parquet("src/test/output/one_record_per_10_mins");
		Dataset<Row> output = object.getAverage(spark, source);

		object.writeData(output, "src/test/output/average/");
	}
	
	/***
	 * Get average
	 * @param spark
	 * @param src
	 * @return
	 */
	public Dataset<Row> getAverage(SparkSession spark, Dataset<Row> src) {

		src.createOrReplaceTempView("src");

		Dataset<Row> output = spark.sql(
				"SELECT timestamp, actions_count_open, actions_count_close, ((actions_count_open + actions_count_close)/10) as average FROM src");

		spark.catalog().dropTempView("src");
		return output;
	}
	
	/***
	 * Save the data as csv.
	 * @param output
	 * @param path
	 */
	public void writeData(Dataset<Row> output, String path) {

		Long counts = output.count();
		if (counts <= 100000)
			output.sort("timestamp").coalesce(1).write().option("header", true).mode(SaveMode.Overwrite).csv(path);
		else if (counts >= 1000000)
			output.sort("timestamp").coalesce(3).write().option("header", true).mode(SaveMode.Overwrite).csv(path);
	}

}
