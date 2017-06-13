package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

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

/**
 * Created by kisho on 2017/6/9.
 */
@ContextConfiguration("classpath:applicationContext.xml")
class ApplyToOrientDBServiceImplTest extends AbstractJUnit4SpringContextTests {

    @Resource
    private Properties kafkaProducerProp

    @Resource
    private Map<String, Map<String, String>> kafkaDispatchConfig

    @Resource
    private ApplyToOrientDBServiceImpl applyTeleporterService

    @Test
    void testProcess() {
        String topic = 'test'

        Schema schema = new Schema.Parser().parse(kafkaDispatchConfig[topic].avroSchema)

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);

        ByteArrayOutputStream oos = new ByteArrayOutputStream()
        dataFileWriter.create(schema, oos)

        GenericRecord record = new GenericData.Record(schema)

        record.put('cellphone', '15821180279')
        record.put('apply_no', '1485313547297000')
        record.put('member_id', 715157L)
        record.put('created_datetime', '2017-06-13 00:00:00')
        record.put('apply_status', 4000)
        record.put('store_id', '2759')
        record.put('order_no', '1496921804405003')
        record.put('__op', 'create') //必须字段

        dataFileWriter.append(record)
        dataFileWriter.close()

        Producer<String, String> producer = new KafkaProducer<>(kafkaProducerProp)
        producer.send(new ProducerRecord<String, Byte[]>(topic, Integer.toString(10000), oos.toByteArray()))
        producer.close()
    }
}
