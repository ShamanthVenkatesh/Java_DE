package com.lidl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Top10MinsOpenAction {
	
	public static void main(String[] args) {
		// Please paste the path appropriately upto the winutils.exe folder to set "hadoop.home.dir" when running locally.
		System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-3.3.1\\");
		Top10MinsOpenAction object = new Top10MinsOpenAction();
		SparkSession spark = SparkSession.builder()
				.master("local[*]")
				//Please comment the above line when running on cluster.
				.getOrCreate();
		
		Dataset<Row> source = spark.read().parquet("src/test/output/one_record_per_10_mins");
		Dataset<Row> output = object.getHighestOpenRec(spark, source);
		
		object.writeData(output, "src/test/output/top10mins_output/");
		
	}
	
	/**
	 * Get higest record with open action.
	 * @param spark
	 * @param src
	 * @return
	 */
	public Dataset<Row> getHighestOpenRec(SparkSession spark, Dataset<Row> src) {
		src.createOrReplaceTempView("src");
		
		Dataset<Row> output = spark.sql("WITH temp AS ( "
				+ "SELECT max(actions_count_open) as max_actions_count_open FROM src) "
				+ "SELECT a.timestamp, a.actions_count_open FROM temp t INNER JOIN src a ON a.actions_count_open = t.max_actions_count_open");
		
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
			output.coalesce(1).write().option("header", true).mode(SaveMode.Overwrite).csv(path);
		else if (counts >= 1000000)
			output.coalesce(3).write().option("header", true).mode(SaveMode.Overwrite).csv(path);
	}
}
