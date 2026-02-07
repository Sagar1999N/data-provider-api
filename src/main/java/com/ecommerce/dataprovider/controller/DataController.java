package com.ecommerce.dataprovider.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;

@RestController
@RequestMapping("/api/v1/data")
public class DataController {

	private static final Logger logger = LoggerFactory.getLogger(DataController.class);
	private static final String ZIP_BASE_DIR = "data/partitioned-zip";

	@GetMapping("/daily")
	public ResponseEntity<Resource> getDailyData(@RequestParam LocalDate date) {
		String dateStr = date.toString();
		logger.info("📥 Request received for daily data: {}", dateStr);

		// Pre-built ZIP path
		Path zipPath = Paths.get(ZIP_BASE_DIR, dateStr + ".zip");

		if (!Files.exists(zipPath)) {
			logger.warn("❌ No data found for date: {}", dateStr);
			return ResponseEntity.notFound().build();
		}

		try {
			FileSystemResource resource = new FileSystemResource(zipPath.toFile());
			String filename = "brazilian-ecommerce-" + dateStr + ".zip";

			HttpHeaders headers = new HttpHeaders();
			headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + filename);
			headers.add(HttpHeaders.CONTENT_TYPE, "application/zip");

			logger.info("✅ Serving data for date: {}", dateStr);
			return ResponseEntity.ok().headers(headers).contentLength(resource.contentLength()).body(resource);

		} catch (Exception e) {
			logger.error("❌ Error serving data for date: {}", dateStr, e);
			return ResponseEntity.internalServerError().build();
		}
	}

	@GetMapping("/customers")
	public ResponseEntity<Resource> getCustomersData() {
		logger.info("📥 Request received for customer data");

		// Pre-built ZIP path
		Path zipPath = Paths.get(ZIP_BASE_DIR, "customers.zip");

		if (!Files.exists(zipPath)) {
			logger.warn("❌ No data found for customers");
			return ResponseEntity.notFound().build();
		}

		try {
			FileSystemResource resource = new FileSystemResource(zipPath.toFile());
			String filename = "brazilian-ecommerce-customers.zip";

			HttpHeaders headers = new HttpHeaders();
			headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + filename);
			headers.add(HttpHeaders.CONTENT_TYPE, "application/zip");

			logger.info("✅ Serving data for customers");
			return ResponseEntity.ok().headers(headers).contentLength(resource.contentLength()).body(resource);

		} catch (Exception e) {
			logger.error("❌ Error serving data for customers", e);
			return ResponseEntity.internalServerError().build();
		}
	}

	@GetMapping("/products")
	public ResponseEntity<Resource> getProductsData() {

		logger.info("📥 Request received for products data");

		// Pre-built ZIP path
		Path zipPath = Paths.get(ZIP_BASE_DIR, "products.zip");

		if (!Files.exists(zipPath)) {
			logger.warn("❌ No data found for products");
			return ResponseEntity.notFound().build();
		}

		try {
			FileSystemResource resource = new FileSystemResource(zipPath.toFile());
			String filename = "brazilian-ecommerce-products.zip";

			HttpHeaders headers = new HttpHeaders();
			headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + filename);
			headers.add(HttpHeaders.CONTENT_TYPE, "application/zip");

			logger.info("✅ Serving data for products");
			return ResponseEntity.ok().headers(headers).contentLength(resource.contentLength()).body(resource);

		} catch (Exception e) {
			logger.error("❌ Error serving data for products", e);
			return ResponseEntity.internalServerError().build();
		}
	}

	@GetMapping("/sellers")
	public ResponseEntity<Resource> getSellersData() {

		logger.info("📥 Request received for sellers data");

		// Pre-built ZIP path
		Path zipPath = Paths.get(ZIP_BASE_DIR, "sellers.zip");

		if (!Files.exists(zipPath)) {
			logger.warn("❌ No data found for sellers");
			return ResponseEntity.notFound().build();
		}

		try {
			FileSystemResource resource = new FileSystemResource(zipPath.toFile());
			String filename = "brazilian-ecommerce-sellers.zip";

			HttpHeaders headers = new HttpHeaders();
			headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + filename);
			headers.add(HttpHeaders.CONTENT_TYPE, "application/zip");

			logger.info("✅ Serving data for sellers");
			return ResponseEntity.ok().headers(headers).contentLength(resource.contentLength()).body(resource);

		} catch (Exception e) {
			logger.error("❌ Error serving data for sellers", e);
			return ResponseEntity.internalServerError().build();
		}
	}

	@GetMapping("/geolocation")
	public ResponseEntity<Resource> getGeolocationData() {
		logger.info("📥 Request received for geolocation data");

		// Pre-built ZIP path
		Path zipPath = Paths.get(ZIP_BASE_DIR, "geolocation.zip");

		if (!Files.exists(zipPath)) {
			logger.warn("❌ No data found for geolocation");
			return ResponseEntity.notFound().build();
		}

		try {
			FileSystemResource resource = new FileSystemResource(zipPath.toFile());
			String filename = "brazilian-ecommerce-geolocation.zip";

			HttpHeaders headers = new HttpHeaders();
			headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + filename);
			headers.add(HttpHeaders.CONTENT_TYPE, "application/zip");

			logger.info("✅ Serving data for geolocation");
			return ResponseEntity.ok().headers(headers).contentLength(resource.contentLength()).body(resource);

		} catch (Exception e) {
			logger.error("❌ Error serving data for geolocation", e);
			return ResponseEntity.internalServerError().build();
		}
	}

	@GetMapping("/product_category_name_translation")
	public ResponseEntity<Resource> getProductCategoryNameTranslationData() {
		logger.info("📥 Request received for product_category_name_translation data");

		// Pre-built ZIP path
		Path zipPath = Paths.get(ZIP_BASE_DIR, "product_category_name_translation.zip");

		if (!Files.exists(zipPath)) {
			logger.warn("❌ No data found for product_category_name_translation");
			return ResponseEntity.notFound().build();
		}

		try {
			FileSystemResource resource = new FileSystemResource(zipPath.toFile());
			String filename = "brazilian-ecommerce-product_category_name_translation.zip";

			HttpHeaders headers = new HttpHeaders();
			headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + filename);
			headers.add(HttpHeaders.CONTENT_TYPE, "application/zip");

			logger.info("✅ Serving data for product_category_name_translation");
			return ResponseEntity.ok().headers(headers).contentLength(resource.contentLength()).body(resource);

		} catch (Exception e) {
			logger.error("❌ Error serving data for product_category_name_translation", e);
			return ResponseEntity.internalServerError().build();
		}
	}
}