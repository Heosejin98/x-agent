package org.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Main {
    public static void main(String[] args) {
        // Kafka 브로커 주소
        String bootstrapServers = "localhost:19092"; // 예: "localhost:9092"

        // Kafka 토픽
        String kafkaTopic = "testsj";

        // Kafka 프로듀서 설정
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Kafka 프로듀서 생성
        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
            String logFilePath = "/home/aiops/xaiops-server/logs/xaiops-server.log";
            while (true) {
                try (BufferedReader reader = new BufferedReader(new FileReader(logFilePath))) {
                    String line;

                    while ((line = reader.readLine()) != null) {
                        // Kafka 토픽으로 로그 전송
                        System.out.println(line);
                        producer.send(new ProducerRecord<>(kafkaTopic, line));
                    }
                } catch (IOException e) {
                    // 에러 발생 시 로깅
                    e.printStackTrace();
                }

                // 1초 대기 후 다시 로그를 읽어오기
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}