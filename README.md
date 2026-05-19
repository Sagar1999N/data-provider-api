# Data Provider API

A Spring Boot REST API that simulates a production data vendor for Brazilian e-commerce data. It serves pre-partitioned Olist dataset files as ZIP downloads over HTTP, providing both daily transactional data and full reference/dimension datasets.

---

## Overview

This service sits between the partitioned data storage (produced by `data-partitioning-job`) and downstream consumers (such as `brazilian-ecommerce-analytics`). Rather than reading from Kaggle directly, downstream jobs call this API to retrieve data as if it were a real vendor API — decoupling data consumers from the source dataset.

```
data-partitioning-job
        │ writes ZIPs to
        ▼
  data/partitioned-zip/
        │
        ▼
  data-provider-api  (this service)
        │ serves over HTTP
        ▼
  downstream consumers (e.g., data-pipeline-orchestrator invoking bronze ingestion)
```

---

## Tech Stack

| Component    | Technology       | Version |
|--------------|------------------|---------|
| Language     | Java             | 17      |
| Framework    | Spring Boot      | 3.2.0   |
| Build Tool   | Maven            | 3.x     |
| Monitoring   | Spring Actuator  | —       |

---

## Project Structure

```
data-provider-api/
├── src/main/java/com/ecommerce/dataprovider/
│   ├── DataProviderApplication.java   # Spring Boot entry point
│   ├── controller/
│   │   └── DataController.java        # REST endpoints
│   └── util/
│       └── ZipUtils.java              # ZIP extraction utility (Zip Slip safe)
├── src/main/resources/
│   ├── application.properties         # Server and data path config
│   └── logback-spring.xml             # Logging configuration
└── pom.xml                            # Maven build config
```

---

## API Endpoints

All endpoints return a ZIP file (`application/zip`, `Content-Disposition: attachment`).

### Daily Data

```
GET /api/v1/data/daily?date={YYYY-MM-DD}
```

Returns a ZIP containing fact table slices (orders, order items, payments, reviews) for the specified date, plus all dimension tables.

**Example:**
```bash
curl -O "http://localhost:8080/api/v1/data/daily?date=2017-10-02"
```

### Reference / Dimension Data

| Endpoint                                                   | Returns               |
|------------------------------------------------------------|-----------------------|
| `GET /api/v1/data/customers`                               | `customers.zip`       |
| `GET /api/v1/data/products`                                | `products.zip`        |
| `GET /api/v1/data/sellers`                                 | `sellers.zip`         |
| `GET /api/v1/data/geolocation`                             | `geolocation.zip`     |
| `GET /api/v1/data/product_category_name_translation`       | Category names ZIP    |

### Health Check

```
GET /actuator/health
```

---

## Prerequisites

- Java 17+
- Maven 3.x
- Pre-built ZIP files in `data/partitioned-zip/` (produced by `data-partitioning-job`)
- Port **8080** available

### Expected Data Layout

```
data/partitioned-zip/
├── 2016-09-04.zip
├── 2016-09-05.zip
├── ...
├── customers.zip
├── products.zip
├── sellers.zip
├── geolocation.zip
└── product_category_name_translation.zip
```

---

## Build

```bash
mvn clean package
```

Produces: `target/data-provider-api-1.0.0.jar`

---

## Run

```bash
# Via Maven
mvn spring-boot:run

# Or as a JAR
java -jar target/data-provider-api-1.0.0.jar
```

Service starts on port **8080** by default.

---

## Configuration

Key settings in `application.properties`:

```properties
server.port=8080
app.name=Data Provider API
app.version=1.0.0
app.data.base-dir=data
app.data.partitioned-dir=${app.data.base-dir}/partitioned-zip
```

---

## Security

`ZipUtils.java` implements Zip Slip vulnerability protection: all extracted file paths are normalized and validated to remain within the target directory before extraction proceeds.

---

## License

Apache 2.0