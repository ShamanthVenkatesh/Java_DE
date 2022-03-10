package com.lidl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTop10MinsOpenAction {
	
	private static Dataset<Row> input;
	private static Dataset<Row> output;
	private static org.apache.spark.sql.SparkSession spark;
	private static Top10MinsOpenAction object;
	
	@BeforeClass
	public static void beforeClass() {
		System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-3.3.1");
		spark = SparkSession.builder().master("local[*]").getOrCreate();
		input = spark.read().parquet("src/test/output/one_record_per_10_mins");
		output = spark.read().option("header", true).csv("src/test/output/top10mins_output/");
		object = new Top10MinsOpenAction();
	}
	
	@Test
	public void checkValidCount() {
		assertEquals(1, output.count());
	}
	
	@Test
	public void checkRecords() {
		Dataset<Row> out = object.getHighestOpenRec(spark, input);
		assertEquals(output.count(), out.count());
	}
	
	@AfterClass
	public static void afterClass() {
		if (spark != null)
			spark.stop();

	}
}
