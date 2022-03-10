package com.lidl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAverageComputationSpark {

	@SuppressWarnings("unused")
	private static Dataset<Row> input;
	private static Dataset<Row> output;
	private static org.apache.spark.sql.SparkSession spark;
	
	@BeforeClass
	public static void beforeClass() {
		System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-3.3.1");
		spark = SparkSession.builder().master("local[*]").getOrCreate();
		input = spark.read().parquet("src/test/output/one_record_per_10_mins");
		output = spark.read().option("header", true).csv("src/test/output/average/").sort("timestamp");
	}
	
	@Test
	public void checkRecords() {
		List<Row> lst = output.collectAsList();
		assertEquals("[201607260100,156,194,35.0]", String.valueOf(lst.get(0)));
		assertNotEquals("[201607260100,156,194,35.0]", String.valueOf(lst.get(1)));
		
	}
	
	@AfterClass
	public static void afterClass() {
		if (spark != null)
			spark.stop();

	}
}
