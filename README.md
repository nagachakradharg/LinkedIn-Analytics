# LinkedIn Job Recommendation System

## System Architecture Overview

### 1.1 Data Storage and Processing
- **Primary Storage**: HBase and Hive for structured and semi-structured data.
- **Data Ingestion**: CSV files uploaded to Hive external tables.
- **Data Format**: ORC for efficient storage and querying.
- **Real-Time Processing**: Kafka for ingesting and processing live data streams.

### 1.2 Key Components

#### Data Ingestion Layer
- **CSV Import**: Hive tables created to manage job postings and associated skills:
  - `nagachakradharg_linkedin_job_postings_csv`
  - `nagachakradharg_linkedin_job_skills_csv`

#### Kafka Streaming
- Simulated job postings data generated and published to Kafka topics using the following components:
  - Attributes such as job level, title, company, and type.
  - `LinkedinKafkaProducer` for generating and sending messages to Kafka.

#### Data Processing Layer
- **ETL Workflow**: CSV data loaded into Hive, transformed into ORC tables for querying.
- **Hive Tables**:
  - `nagachakradharg_linkedin_job_postings`
  - `nagachakradharg_linkedin_job_skills`

## Analytics Features

### 2.1 Geographic Analysis

####Location-Level Metrics

-**Metrics**:
  - Job count per location
  - Average job posting duration (in days)
  - Remote job count
  - Number of unique companies
  - Most common job type
-**Implementation**:
  - HBase table: nagachakradharg_linkedin_location_heat_map_hbase
  - Query aggregates by job_location to compute the above metrics, ensuring proper handling of NULL values and empty strings for accurate results.

--TO be implemented
#### Country-Level Metrics
- **Metrics**:
  - Open job count
  - Total job count
  - Average job duration
- **Implementation**:
  - HBase table: `nagachakradharg_linkedin_countrywide_features_hbase`
  - Query aggregates by `search_country` to calculate metrics.

#### City-Level Metrics
- Similar metrics aggregated by `search_city`.
- **HBase Table**: `nagachakradharg_linkedin_citywide_features_hbase`

### 2.2 Temporal Analysis
#### Year-Over-Month Trends
- **Metrics**:
  - Average job durations for each month.
- **Implementation**:
  - HBase table: `nagachakradharg_linkedin_job_year_features_hbase`
  - Aggregated by `first_seen` and `last_processed_time`.

### 2.3 Company Analysis
- **Metrics**:
  - Job level distribution.
  - Total job postings per company.
- **Implementation**:
  - HBase table: `nagachakradharg_top_companies_by_level`.
  - Query job levels and sort by job posting frequency.

### 2.4 Skills Analysis
#### Top Skills by Job Level
- **Metrics**:
  - Frequency of skills at each job level.
- **Implementation**:
  - Explode `job_skills` column into individual skills.
  - Aggregate skill counts grouped by job level.
  - HBase table: `nagachakradharg_linkedin_top_skills_level_hbase`.

## Real-Time Processing

### 3.1 Kafka Implementation
#### Producer
- Generates simulated job posting data with attributes including:
  - Job level, title, company, type, location, and timestamps.
- Publishes to Kafka topic `nagachakradharg_linkedin_livefeed`.

### 3.2 Stream Processing
- Spark Streaming processes Kafka data in real-time.
- Real-time job postings appended to HBase tables for immediate analytics.


### Frontend
- Connection issue yet to be fixed
- Aggregates batch and speed layer outputs.
- Displays shuffled recommendations.
- Captures user interactions (e.g., skips, likes) and publishes them to Kafka for feedback.

## Implementation Details

### Data Definitions
#### Hive Table for Job Postings
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS nagachakradharg_linkedin_job_postings_csv (
    job_link            STRING,
    last_processed_time STRING,
    got_summary         STRING,
    got_ner             STRING,
    is_being_worked     STRING,
    job_title           STRING,
    company             STRING,
    job_location        STRING,
    first_seen          STRING,
    search_city         STRING,
    search_country      STRING,
    search_position     STRING,
    job_level           STRING,
    job_type            STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '<azure_blob_storage_path>'
TBLPROPERTIES ("skip.header.line.count"="1");
```

#### Kafka Producer
```java
KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
// Generate random job data and send to Kafka topic.
for (int i = 0; i < numRecords; i++) {
    String message = String.format(...);
    producer.send(new ProducerRecord<>(TOPIC, jobLink, message));
}
```

## Conclusion
The LinkedIn Job Recommendation System integrates batch and stream processing for robust, real-time analytics about job postings and skills, leveraging Hive, HBase, and Kafka for scalable data storage and computation.

Find all the jars and Datasets in the folder /nagachakradharg/Project Data
