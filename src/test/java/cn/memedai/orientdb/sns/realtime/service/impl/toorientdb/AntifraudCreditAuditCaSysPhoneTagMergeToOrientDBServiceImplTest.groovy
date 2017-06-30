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
 * Created by hangyu on 2017/6/20.
 */
@ContextConfiguration("classpath:applicationContext.xml")
class AntifraudCreditAuditCaSysPhoneTagMergeToOrientDBServiceImplTest extends AbstractJUnit4SpringContextTests{
    @Resource
    private Properties kafkaProducerProp

    @Resource
    private Map<String, Map<String, Map<String, String>>> kafkaDispatchConfig

    @Test
    void testProcess() {
        String topic = 'antifraud'

        Schema schema = new Schema.Parser().parse(kafkaDispatchConfig[topic]['phone_tag_merge_crawler_all'].avroSchema)

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);

        ByteArrayOutputStream oos = new ByteArrayOutputStream()
        dataFileWriter.create(schema, oos)

        GenericRecord record = new GenericData.Record(schema)

        record.put('__schemaid__', '123456')
        record.put('phone_no', '13001007936')
        record.put('phone_type', '贷款中介1')
        record.put('source', '59')
        record.put('__op__', 'insert') //必须字段

        dataFileWriter.append(record)
        dataFileWriter.close()

        Producer<String, String> producer = new KafkaProducer<>(kafkaProducerProp)
        [0..10].each {
            producer.send(new ProducerRecord<String, Byte[]>(topic, 'phone_tag_merge_crawler_all', oos.toByteArray()))
        }
        producer.close()
    }
}
