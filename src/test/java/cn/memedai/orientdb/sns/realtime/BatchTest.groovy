package cn.memedai.orientdb.sns.realtime

import groovy.json.JsonSlurper
import groovy.sql.Sql
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumWriter
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.Test
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests

import javax.annotation.Resource
import java.sql.Timestamp

@ContextConfiguration("classpath:applicationContext.xml")
class BatchTest extends AbstractJUnit4SpringContextTests {

    @Resource
    private Properties kafkaProducerProp

    @Resource
    private Map<String, Map<String, Map<String, String>>> kafkaDispatchConfig

    @Resource
    private Sql groovySql

    @Test
    void batchTest() {
        Producer<String, String> producer = new KafkaProducer<>(kafkaProducerProp)
        producer.send(new ProducerRecord<String, Byte[]>(
                'wallet',
                'wallet.apply_info',
                getData('select * from network.apply_info where id >= 643473',
                        'wallet',
                        'wallet.apply_info')
        )
        )
        //TODO
        producer.close()
    }

    private byte[] getData(String sql, String topic, String key) {
        Schema schema = new Schema.Parser().parse(kafkaDispatchConfig[topic][key].avroSchema)

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema)
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter)

        ByteArrayOutputStream oos = new ByteArrayOutputStream()
        dataFileWriter.create(schema, oos)

        groovySql.eachRow(sql,
                {
                    row ->
                        GenericRecord record = new GenericData.Record(schema)
                        record.put('__schemaid__', '123456')
                        record.put('__op__', 'insert')
                        schema['fields'].each {
                            field ->
                                if (field['name'] == '__schemaid__' || field['name'] == '__op__') {
                                    return
                                }
                                def fieldValue = row.(field['name'])
                                if (fieldValue != null && fieldValue instanceof Timestamp) {
                                    fieldValue = fieldValue.toString()
                                }
                                record.put(field['name'], fieldValue == null ? null : fieldValue)
                        }

                        dataFileWriter.append(record)
                })

        dataFileWriter.close()

        oos.toByteArray()
    }

    @Test
    void test1() {
        def schemaData = new JsonSlurper().parseText(kafkaDispatchConfig['wallet']['wallet.apply_info'].avroSchema)
        groovySql.eachRow('select * from network.apply_info limit 10', {
            row ->
                schemaData['fields'].each {
                    filed ->
                        if (filed['name'] == '__schemaid__' || filed['name'] == '__op__') {
                            return
                        }
                        println(row.(filed['name']))
                }
        })
    }

}