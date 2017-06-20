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
 * Created by hangyu on 2017/6/14.
 */
@ContextConfiguration("classpath:applicationContext.xml")
class StoreToOrientDBServiceImplTest extends AbstractJUnit4SpringContextTests{
    @Resource
    private Properties kafkaProducerProp

    @Resource
    private Map<String, Map<String, String>> kafkaDispatchConfig

    @Test
    void testProcess() {
        String topic = 'store'

        Schema schema = new Schema.Parser().parse(kafkaDispatchConfig[topic].avroSchema)

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);

        ByteArrayOutputStream oos = new ByteArrayOutputStream()
        dataFileWriter.create(schema, oos)

        GenericRecord record = new GenericData.Record(schema)

        record.put('STOREID', 1)
        record.put('MERCHANTID', 100634)
        record.put('STORENAME', '新街口店')
        record.put('PROVINCE', '江苏省')
        record.put('CITY', '南京市')
        record.put('CREDIT_LIMIT_TYPE', 1)
        record.put('__op__', 'create') //必须字段

        dataFileWriter.append(record)
        dataFileWriter.close()

        Producer<String, String> producer = new KafkaProducer<>(kafkaProducerProp)
        producer.send(new ProducerRecord<String, Byte[]>(topic, Integer.toString(10000), oos.toByteArray()))
        producer.close()
    }
}
