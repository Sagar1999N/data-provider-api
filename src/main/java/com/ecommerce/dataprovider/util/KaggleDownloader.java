package com.ecommerce.dataprovider.util;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;

/**
 * Downloads dataset from Kaggle using API token or legacy credentials.
 */
@Component
public class KaggleDownloader {

    private static final Logger logger = LoggerFactory.getLogger(KaggleDownloader.class);
    private static final String DATASET_ZIP = "brazilian-ecommerce.zip";

    @Value("${app.data.base-dir}")
    private String baseDir;

    @Value("${kaggle.dataset}")
    private String dataset;

    @Value("${kaggle.username:#{null}}")
    private String username;

    @Value("${kaggle.key:#{null}}")
    private String key;

    public void downloadDataset() {
        Path zipPath = Paths.get(baseDir, DATASET_ZIP);
        if (Files.exists(zipPath)) {
            logger.info("📁 Dataset already exists at: {}", zipPath);
            return;
        }

        // Create base directory
        try {
            Files.createDirectories(Paths.get(baseDir));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create base directory", e);
        }

        if (username != null && key != null) {
            downloadWithLegacyAuth(zipPath);
        } else {
            String token = System.getenv("KAGGLE_API_TOKEN");
            if (token != null) {
                downloadWithApiToken(zipPath, token);
            } else {
                throw new IllegalStateException("Kaggle credentials not found. Set KAGGLE_API_TOKEN or kaggle.username/key");
            }
        }
    }

    private void downloadWithApiToken(Path zipPath, String token) {
        logger.info("⬇️ Downloading dataset using API token");
        String url = "https://www.kaggle.com/api/v1/datasets/download/" + dataset;

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet request = new HttpGet(url);
            request.setHeader("Authorization", "Bearer " + token);

            HttpResponse response = httpClient.execute(request);
            handleResponse(response, zipPath);
        } catch (Exception e) {
            throw new RuntimeException("Failed to download dataset with API token", e);
        }
    }

    private void downloadWithLegacyAuth(Path zipPath) {
        logger.info("⬇️ Downloading dataset using legacy credentials");
        String url = "https://www.kaggle.com/api/v1/datasets/download/" + dataset;
        String auth = username + ":" + key;
        String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet request = new HttpGet(url);
            request.setHeader("Authorization", "Basic " + encodedAuth);

            HttpResponse response = httpClient.execute(request);
            handleResponse(response, zipPath);
        } catch (Exception e) {
            throw new RuntimeException("Failed to download dataset with legacy credentials", e);
        }
    }

    private void handleResponse(HttpResponse response, Path zipPath) throws IOException {
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != 200) {
            String body = EntityUtils.toString(response.getEntity());
            throw new RuntimeException("Kaggle API returned " + statusCode + ": " + body);
        }

        HttpEntity entity = response.getEntity();
        if (entity != null) {
            try (InputStream is = entity.getContent();
                 FileOutputStream fos = new FileOutputStream(zipPath.toFile())) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = is.read(buffer)) != -1) {
                    fos.write(buffer, 0, bytesRead);
                }
            }
            logger.info("✅ Dataset downloaded to: {}", zipPath);
        }
    }
}