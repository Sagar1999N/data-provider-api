package com.ecommerce.dataprovider;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Data Provider API - serves partitioned Brazilian e-commerce data by date.
 * On startup, downloads and partitions the dataset once.
 */
@SpringBootApplication
public class DataProviderApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataProviderApplication.class, args);
    }
}