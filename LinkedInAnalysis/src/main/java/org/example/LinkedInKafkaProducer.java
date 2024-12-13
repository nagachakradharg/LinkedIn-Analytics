package org.example;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class LinkedInKafkaProducer {

    private static final String TOPIC = "nagachakradharg_linkedin_livefeed"; // Replace with your Kafka topic name
    private static final String KAFKABROKERS = "wn0-kafka.m0ucnnwuiqae3jdorci214t2mf.bx.internal.cloudapp.net:9092,wn1-kafka.m0ucnnwuiqae3jdorci214t2mf.bx.internal.cloudapp.net:9092";
    // Generate random data
    private static final String[] jobLevels = {"Associate", "Mid Senior", "Senior", "Manager", "Director"};
    private static final String[] jobTitles = {"Software Engineer", "Data Scientist", "Product Manager", "HR Manager", "Business Analyst"};
    private static final String[] companies = {"Company A", "Company B", "Company C", "Company D", "Company E"};
    private static final String[] jobTypes = {"Full-time", "Part-time", "Contract", "Internship"};

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKABROKERS);
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        Random random = new Random();
        int numRecords = 1000;  // Number of records to generate

        for (int i = 0; i < numRecords; i++) {
            String jobLink = "job-" + UUID.randomUUID().toString();
            String lastProcessedTime = generateRandomDate();
            String gotSummary = random.nextBoolean() ? "true" : "false";
            String gotNer = random.nextBoolean() ? "true" : "false";
            String isBeingWorked = random.nextBoolean() ? "true" : "false";
            String jobTitle = jobTitles[random.nextInt(jobTitles.length)];
            String company = companies[random.nextInt(companies.length)];
            String jobLocation = "Location " + random.nextInt(100);
            String firstSeen = generateRandomDate();
            String searchCity = "City " + random.nextInt(100);
            String searchCountry = "Country " + random.nextInt(100);
            String searchPosition = "Position " + random.nextInt(100);
            String jobLevel = jobLevels[random.nextInt(jobLevels.length)];
            String jobType = jobTypes[random.nextInt(jobTypes.length)];

            String message = String.format("{\"job_link\":\"%s\",\"last_processed_time\":\"%s\",\"got_summary\":\"%s\",\"got_ner\":\"%s\"," +
                            "\"is_being_worked\":\"%s\",\"job_title\":\"%s\",\"company\":\"%s\",\"job_location\":\"%s\",\"first_seen\":\"%s\"," +
                            "\"search_city\":\"%s\",\"search_country\":\"%s\",\"search_position\":\"%s\",\"job_level\":\"%s\",\"job_type\":\"%s\"}",
                    jobLink, lastProcessedTime, gotSummary, gotNer, isBeingWorked, jobTitle, company, jobLocation, firstSeen,
                    searchCity, searchCountry, searchPosition, jobLevel, jobType);

            producer.send(new ProducerRecord<>(TOPIC, jobLink, message));
            System.out.println("Produced record: " + message);
        }

        producer.close();
    }

    private static String generateRandomDate() {
        int year = 2020 + new Random().nextInt(5);  // Random year between 2020 and 2025
        int month = 1 + new Random().nextInt(12);    // Random month between 1 and 12
        int day = 1 + new Random().nextInt(28);      // Random day between 1 and 28 (to avoid invalid dates)

        return String.format("%d-%02d-%02d", year, month, day);
    }
}
