package com.ecommerce.dataprovider.service;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.ecommerce.dataprovider.util.KaggleDownloader;
import com.ecommerce.dataprovider.util.ZipUtils;

import jakarta.annotation.PostConstruct;

/**
 * Service to download, extract, and partition the Brazilian e-commerce dataset.
 * Runs once on application startup.
 */
@Service
public class DataPartitioningService {

	private static final Logger logger = LoggerFactory.getLogger(DataPartitioningService.class);

	@Value("${app.data.base-dir}")
	private String baseDir;

	@Value("${app.data.extracted-dir}")
	private String extractedDir;

	@Value("${app.data.partitioned-dir}")
	private String partitionedDir;

	private final KaggleDownloader kaggleDownloader;
	private final ZipUtils zipUtils;

	public DataPartitioningService(KaggleDownloader kaggleDownloader, ZipUtils zipUtils) {
		this.kaggleDownloader = kaggleDownloader;
		this.zipUtils = zipUtils;
	}

	@PostConstruct
	public void initialize() {
		logger.info("🚀 Initializing Data Provider API...");

		try {
			// Step 1: Download dataset if not present
			kaggleDownloader.downloadDataset();

			// Step 2: Extract ZIP if not extracted
			extractDatasetIfNeeded();

			// Step 3: Partition data by date if not partitioned
			partitionDataByDateIfNeeded();

			logger.info("✅ Data Provider API ready to serve data!");
		} catch (Exception e) {
			logger.error("❌ Failed to initialize data provider", e);
			throw new RuntimeException("Initialization failed", e);
		}
	}

	private void extractDatasetIfNeeded() throws Exception {
		Path extractedPath = Paths.get(extractedDir);
		if (Files.exists(extractedPath)) {
			logger.info("📁 Dataset already extracted at: {}", extractedDir);
			return;
		}

		logger.info("📦 Extracting dataset...");
		String zipPath = baseDir + "/brazilian-ecommerce.zip";
		zipUtils.unzip(zipPath, extractedDir);
	}

	private void partitionDataByDateIfNeeded() throws Exception {
		Path partitionedPath = Paths.get(partitionedDir);
		if (Files.exists(partitionedPath) && Files.list(partitionedPath).count() > 0) {
			logger.info("📁 Data already partitioned at: {}", partitionedDir);
			return;
		}

		logger.info("✂️ Partitioning data by order_purchase_date...");
		partitionDataByDate();
	}

	private void partitionDataByDate() {
		SparkSession spark = SparkSession.builder().appName("DataPartitioning").master("local[*]")
				.config("spark.sql.session.timeZone", "UTC").getOrCreate();

		try {
			// List of all tables to partition
			String[] tables = { "olist_orders_dataset.csv", "olist_order_items_dataset.csv",
					"olist_order_payments_dataset.csv", "olist_order_reviews_dataset.csv",
					"olist_customers_dataset.csv", "olist_products_dataset.csv", "olist_sellers_dataset.csv" };

			// Read orders to get all dates
			Dataset<Row> orders = spark.read().option("header", "true").option("inferSchema", "true")
					.csv(extractedDir + "/olist_orders_dataset.csv").withColumn("order_date",
							org.apache.spark.sql.functions.to_date(
									org.apache.spark.sql.functions.col("order_purchase_timestamp"),
									"yyyy-MM-dd HH:mm:ss"));

			List<LocalDate> dates = orders.select("order_date").dropDuplicates().collectAsList().stream()
					.map(row -> row.getDate(0).toLocalDate()).collect(Collectors.toList());

			logger.info("🗓️ Found {} unique order dates", dates.size());

			// Partition each date
			for (LocalDate date : dates) {
				String dateStr = date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
				logger.info("✂️ Partitioning data for date: {}", dateStr);

				// Partition each table for this date
				for (String table : tables) {
					String entityName = table.replace("olist_", "").replace("_dataset.csv", "");
					partitionTableForDate(spark, date, table, entityName);
				}
			}

		} finally {
			spark.stop();
		}
	}

	private void partitionTableForDate(SparkSession spark, LocalDate date, String sourceFile, String entityName) {
		String dateStr = date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
		String outputPath = partitionedDir + "/" + dateStr + "/" + entityName + "/";

		// Ensure output directory exists
		new File(outputPath).mkdirs();

		// Read source table
		Dataset<Row> df = spark.read().option("header", "true").option("inferSchema", "true")
				.csv(extractedDir + "/" + sourceFile);

		// Filter by date
		if ("orders".equals(entityName)) {
			df = df.filter(org.apache.spark.sql.functions
					.to_date(org.apache.spark.sql.functions.col("order_purchase_timestamp"), "yyyy-MM-dd HH:mm:ss")
					.equalTo(dateStr));
		} else if (Arrays.asList("order_items", "order_payments", "order_reviews").contains(entityName)) {
			// Join with orders to get date
			Dataset<Row> orders = spark.read().option("header", "true").option("inferSchema", "true")
					.csv(extractedDir + "/olist_orders_dataset.csv")
					.withColumn("order_date", org.apache.spark.sql.functions.to_date(
							org.apache.spark.sql.functions.col("order_purchase_timestamp"), "yyyy-MM-dd HH:mm:ss"))
					.select("order_id", "order_date");

			df = df.join(orders, "order_id").filter(org.apache.spark.sql.functions.col("order_date").equalTo(dateStr))
					.drop("order_date");
		}
		// For dimension tables (customers, products, sellers), include in every date
		// (simplification for demo - in prod, you'd handle SCD)

		// Write partitioned data
		if (df.count() > 0) {
			df.coalesce(1) // Single file per table per date
					.write().mode("overwrite").option("header", "true").csv(outputPath);
			logger.info("💾 Wrote {} records to {}", df.count(), outputPath);
		}
	}
}