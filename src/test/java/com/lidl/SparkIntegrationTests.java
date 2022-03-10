package com.lidl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SparkIntegrationTests {

	private static org.apache.spark.sql.SparkSession spark;

	private static SparkChallenge sparkChallenge;
	private static AverageComputationSpark averageComputationSpark;
	private static Top10MinsOpenAction top10MinsOpenAction;

	private static Dataset<Row> input_events;

	@BeforeClass
	public static void beforeClass() {
		System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-3.3.1");
		spark = SparkSession.builder().master("local[*]").getOrCreate();
		input_events = spark.read().option("header", true).csv("src/test/resources/events.csv");
	}

	@Test
	public void integrateTest() {

		input_events.createOrReplaceTempView("input");

		Dataset<Row> integration_test_data = spark.sql("SELECT * FROM input ORDER BY time LIMIT 100");
		integration_test_data.repartition(1).write().mode(SaveMode.Overwrite).option("header", true)
				.csv("src/test/resources/integration/events_test_data");
		spark.catalog().dropTempView("input");

		// Build BaseData
		sparkChallenge = new SparkChallenge();
		Dataset<Row> baseData = sparkChallenge.getInterimBaseRecords(spark, integration_test_data);
		sparkChallenge.writeData(baseData, "src/test/resources/integration/baseData");
		Dataset<Row> oneRecordPerRow = sparkChallenge.getOneRowForTenMinutes(spark, baseData);
		sparkChallenge.writeData(oneRecordPerRow, "src/test/resources/integration/one_record_per_10_mins");

		// Compute Average
		averageComputationSpark = new AverageComputationSpark();

		Dataset<Row> records_per_10mins = spark.read().parquet("src/test/resources/integration/one_record_per_10_mins");
		Dataset<Row> average = averageComputationSpark.getAverage(spark, records_per_10mins);
		averageComputationSpark.writeData(average, "src/test/resources/integration/average/");
		
		top10MinsOpenAction = new Top10MinsOpenAction();
		Dataset<Row> top10Mins = top10MinsOpenAction.getHighestOpenRec(spark, records_per_10mins);
		top10MinsOpenAction.writeData(top10Mins, "src/test/resources/integration/top10Mins/");
	}

	@AfterClass
	public static void afterClass() {
		if (spark != null)
			spark.stop();

	}
}
