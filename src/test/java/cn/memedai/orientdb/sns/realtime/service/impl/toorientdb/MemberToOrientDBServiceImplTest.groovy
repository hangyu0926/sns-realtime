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
 * Created by hangyu on 2017/6/15.
 */
@ContextConfiguration("classpath:applicationContext.xml")
class MemberToOrientDBServiceImplTest extends AbstractJUnit4SpringContextTests{
    @Resource
    private Properties kafkaProducerProp

    @Resource
    private Map<String, Map<String, String>> kafkaDispatchConfig

    @Test
    void testProcess() {
        String topic = 'member'

        Schema schema = new Schema.Parser().parse(kafkaDispatchConfig[topic].avroSchema)

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);

        ByteArrayOutputStream oos = new ByteArrayOutputStream()
        dataFileWriter.create(schema, oos)

        GenericRecord record = new GenericData.Record(schema)

        record.put('MEMBER_ID', 1695737)
        record.put('MOBILE_NO', '18847731939')
        record.put('NAME', '余永莲1')
        record.put('ID_NO', '152726199503290028')
        record.put('PROVINCE', '内蒙古1')
        record.put('CITY', '伊克昭1')
        record.put('__op', 'create') //必须字段

        dataFileWriter.append(record)
        dataFileWriter.close()

        Producer<String, String> producer = new KafkaProducer<>(kafkaProducerProp)
        producer.send(new ProducerRecord<String, Byte[]>(topic, Integer.toString(10000), oos.toByteArray()))
        producer.close()
    }
}
