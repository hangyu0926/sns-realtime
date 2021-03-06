package cn.memedai.orientdb.sns.realtime

import groovy.json.JsonSlurper
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.junit.Before
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

    protected Map<String, Schema> dbtable2SchemaMap = [:]

    @Before
    void setup() {
        JsonSlurper jsonSlurper = new JsonSlurper()
        new File("${getClass().getResource('/').toString()}avsc".replaceFirst('file:', '')).listFiles().each {
            avscFile ->
                Map schemaMap = jsonSlurper.parseText(avscFile.text)
                dbtable2SchemaMap["${schemaMap.namespace}${schemaMap.name}"] = new Schema.Parser().parse(avscFile.text)
        }
    }

    protected Schema getSchema(String topic, String table) {
        dbtable2SchemaMap["$topic$table"]
    }

    protected void produce(String topic, String table, List<Map> dataList) {
        Schema schema = getSchema(topic, table)

        Producer<String, String> producer = new KafkaProducer<>(kafkaProducerProp)

        dataList.each {
            data ->
                GenericRecord record = new GenericData.Record(schema)
                data.each {
                    key, value ->
                        record.put(key, value)
                }
                if (!data.containsKey('___schemaid___')) {
                    record.put('___schemaid___', 1)
                }
                if (!data.containsKey('___op___')) {
                    record.put('___op___', 'insert')
                }

                record.iterator().each {
                    print(it)
                }

//                ByteArrayOutputStream baos = new ByteArrayOutputStream()
//                DatumWriter writer = new SpecificDatumWriter(schema)
//                BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null)
//
//                writer.write(record, encoder)
//                encoder.flush()
//                baos.flush()
//
//                producer.send(new ProducerRecord<String, Byte[]>(topic, table, baos.toByteArray()))
        }
        LOG.info("send successfully")
        producer.close()
    }

}
