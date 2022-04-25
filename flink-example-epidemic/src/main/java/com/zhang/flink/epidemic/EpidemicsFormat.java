package com.zhang.flink.epidemic;


import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;
@Slf4j
public class EpidemicsFormat implements OutputFormat<EpidemicModel> {
    private Properties properties;
    private KafkaProducer<String,EpidemicModel> producer;
    @Override
    public void configure(Configuration parameters) {
        this.properties = new Properties();
        this.properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop101:9092");
        this.properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,EpidemicModel.JSONSerializer.class.getName());
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        this.producer = new KafkaProducer<>(properties);
    }

    @Override
    public void writeRecord(EpidemicModel record) throws IOException {
        this.producer.send(new ProducerRecord<>("epidemic2", record));
        log.info("已推送"+record);
    }

    @Override
    public void close() throws IOException {
        this.producer.close();
    }
}
