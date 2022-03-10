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

public class TestSparkChallenge {

	private static Dataset<Row> input;
	private static Dataset<Row> base;
	private static Dataset<Row> output;
	private static org.apache.spark.sql.SparkSession spark;

	@BeforeClass
	public static void beforeClass() {
		System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-3.3.1");
		spark = SparkSession.builder().master("local[*]").getOrCreate();
		input = spark.read().option("header", true).csv("src/test/resources/events.csv");
		base = spark.read().parquet("src/test/output/base_data").sort("timestamp");
		output = spark.read().parquet("src/test/output/one_record_per_10_mins");
	}

	@Test
	public void checkCountOfRecordsValid() {
		Dataset<Row> df1 = input.select("*");
		assertEquals(df1.count(), input.count());
	}

	@Test
	public void checkCountOfRecordsInvalid() {
		assertNotEquals(100, input.count());
	}

	@Test
	public void finalOutputTestValid() {
		List<Row> lst = output.collectAsList();
		String value[] = String.valueOf(lst.get(0)).split(",");
		assertEquals("[201607260100", value[0]);
		assertEquals("156", value[1]);
		assertEquals("194]", value[2]);
	}

	@Test
	public void baseDataValid() {
		List<Row> lst = base.collectAsList();
		String value[] = String.valueOf(lst.get(0)).split(",");
		assertEquals("[201607260100", value[0]);
		assertEquals("26", value[1]);
		assertEquals("30]", value[2]);
	}

	@AfterClass
	public static void afterClass() {
		if (spark != null)
			spark.stop();

	}
}
