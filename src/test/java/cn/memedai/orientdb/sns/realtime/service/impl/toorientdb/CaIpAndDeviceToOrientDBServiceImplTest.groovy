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
class CaIpAndDeviceToOrientDBServiceImplTest extends AbstractJUnit4SpringContextTests{

    @Resource
    private Properties kafkaProducerProp

    @Resource
    private Map<String, Map<String, Map<String, String>>> kafkaDispatchConfig

    @Test
    void testProcess() {
        String topic = 'credit_audit'

        Schema schema = new Schema.Parser().parse(kafkaDispatchConfig[topic]['credit_audit.ca_appl_member_device'].avroSchema)

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);

        ByteArrayOutputStream oos = new ByteArrayOutputStream()
        dataFileWriter.create(schema, oos)

        GenericRecord record = new GenericData.Record(schema)

        record.put('APPL_NO', '0320018345966835')
        record.put('DEVICE_ID', '862095026190972')
        record.put('IP', '221.226.85.146')
        record.put('IP_CITY', '南京市')
        record.put('__op', 'insert') //必须字段

        dataFileWriter.append(record)
        dataFileWriter.close()

        Producer<String, String> producer = new KafkaProducer<>(kafkaProducerProp)
        [0..10].each {
            producer.send(new ProducerRecord<String, Byte[]>(topic, ['credit_audit.ca_appl_member_device'], oos.toByteArray()))
        }
        producer.close()
    }
}
