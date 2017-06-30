package cn.memedai.orientdb.sns.realtime

import groovy.json.JsonSlurper
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumWriter
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests

import javax.annotation.Resource

/**
 * Created by kisho on 2017/6/30.
 */
@ContextConfiguration("classpath:applicationContext.xml")
abstract class AbstractRealTimeTest extends AbstractJUnit4SpringContextTests {

    private final Logger LOG = LoggerFactory.getLogger(getClass())

    @Resource
    private Properties kafkaProducerProp

    protected Schema getSchema(String topic, String table) {
        JsonSlurper jsonSlurper = new JsonSlurper()
        Schema schema = null
        new File("${getClass().getResource('/').toString()}avsc".replaceFirst('file:', '')).listFiles().each {
            avscFile ->
                if (schema != null) {
                    return
                }
                Map schemaMap = jsonSlurper.parseText(avscFile.text)
                String topic1 = schemaMap.namespace.substring(schemaMap.namespace.lastIndexOf('.') + 1)
                String table1 = schemaMap.name
                if (topic1 == topic && table1 == table) {
                    schema = new Schema.Parser().parse(avscFile.text)
                }
        }
        schema
    }

    protected void produce(String topic, String table, List<Map> dataList) {
        org.apache.avro.Schema schema = getSchema(topic, table)

        Producer<String, String> producer = new KafkaProducer<>(kafkaProducerProp)

        dataList.each {
            data ->
                DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema)
                DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter)

                ByteArrayOutputStream oos = new ByteArrayOutputStream()
                dataFileWriter.create(schema, oos)

                GenericRecord record = new GenericData.Record(schema)
                data.each {
                    key, value ->
                        record.put(key, value)
                }
                dataFileWriter.append(record)
                dataFileWriter.close()
                producer.send(new ProducerRecord<String, Byte[]>(topic, table, oos.toByteArray()))
        }
        LOG.info("send successfully")
        producer.close()
    }

}
