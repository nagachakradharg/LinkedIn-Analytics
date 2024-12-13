package org.example;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class LinkedInSparkProcessor {
    private static final Logger logger = LoggerFactory.getLogger(LinkedInSparkProcessor.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String HBASE_TABLE = "nagachakradharg_linkedin_job_postings";

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: LinkedInSparkProcessor <brokers> <batchIntervalSeconds>");
            System.exit(1);
        }

        String brokers = args[0];
        int batchInterval = Integer.parseInt(args[1]);

        // Initialize Spark
        SparkConf sparkConf = new SparkConf().setAppName("nagachakradharg_linkedin_processor");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Seconds.apply(batchInterval));
        SparkSession spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();

        // Initialize HBase
        Configuration hbaseConf = HBaseConfiguration.create();
        Connection hbaseConnection = ConnectionFactory.createConnection(hbaseConf);
        Table hbaseTable = hbaseConnection.getTable(TableName.valueOf(HBASE_TABLE));

        // Kafka parameters
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "linkedin_processor_group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        // Create Kafka stream and process
        KafkaUtils.createDirectStream(
            jssc,
            LocationStrategies.PreferConsistent(),
            ConsumerStrategies.<String, String>Subscribe(
                Collections.singleton("nagachakradharg_linkedin_livefeed"),
                kafkaParams
            )
        ).foreachRDD((rdd, time) -> {
            if (!rdd.isEmpty()) {
                OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

                try {
                    // Process each partition
                    rdd.foreachPartition(partition -> {
                        partition.forEachRemaining(record -> {
                            try {
                                // Parse JSON
                                Map<String, Object> jobData = mapper.readValue(
                                    record.value().toString(),
                                    new TypeReference<Map<String, Object>>() {}
                                );
                                String jobLink = (String) jobData.get("job_link");

                                // Create HBase Put
                                Put put = new Put(Bytes.toBytes(jobLink));
                                
                                // Add all fields to HBase
                                jobData.forEach((key, value) -> {
                                    if (value != null) {
                                        put.addColumn(
                                            Bytes.toBytes("details"),
                                            Bytes.toBytes(key),
                                            Bytes.toBytes(value.toString())
                                        );
                                    }
                                });

                                // Write to HBase
                                hbaseTable.put(put);
                                logger.info("Stored job posting: {}", jobLink);

                            } catch (Exception e) {
                                logger.error("Error processing record: {}", record.value(), e);
                            }
                        });
                    });

                    // Commit offsets after successful processing
                    ((CanCommitOffsets) rdd.rdd()).commitAsync(offsetRanges);
                    logger.info("Successfully processed and committed offsets");

                } catch (Exception e) {
                    logger.error("Error processing RDD", e);
                }
            }
        });

        // Start processing
        jssc.start();
        jssc.awaitTermination();
    }
}