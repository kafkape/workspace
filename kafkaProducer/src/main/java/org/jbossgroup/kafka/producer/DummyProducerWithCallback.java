package org.jbossgroup.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Properties;

public class DummyProducerWithCallback {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(DummyProducerWithCallback.class);

        String bootstrapServers = "192.168.56.101:9092,192.168.56.102:9092,192.168.56.103:9092,";
        // Propiedades de la conexion
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // creacion del productor
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // crea el record
        for(int i=0; i < 5; i++) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("dummy_topic", "Hola Gente! "+i+
                                                " Desde Java Kafka Producer con Callback " + new Date());
            // envia los datos
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info(" Recibiendo la metadata. \n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset:" + recordMetadata.offset() + "\n" +
                                "Timestamp:" + recordMetadata.timestamp());
                    } else {
                        logger.error("Error mientras se procesa", e);
                    }
                }
            });
            // producer es asyncrono - flush
            producer.flush();
            System.out.println("Record "+i+" Enviado a " + bootstrapServers);
        }
        producer.close();
    }

}
