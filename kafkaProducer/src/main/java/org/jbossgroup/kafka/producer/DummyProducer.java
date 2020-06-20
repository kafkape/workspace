package org.jbossgroup.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Date;
import java.util.Properties;

public class DummyProducer {

    public static void main(String[] args) {

        String bootstrapServers ="192.168.56.101:9092";
        // Propiedades de la conexion
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        // creacion del productor
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        // crea el record
        ProducerRecord<String,String> record =
                new ProducerRecord<String, String>("dummy_topic","Hello from Java Kafka Producer "+ new Date());

        // envia los datos
        producer.send(record);

        // producer es asyncrono
        producer.flush();
        producer.close();

        System.out.println("Record Enviado a "+bootstrapServers);
    }

}
