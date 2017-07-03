package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

import cn.memedai.orientdb.sns.realtime.AbstractRealTimeTest
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

/**
 * Created by hangyu on 2017/6/15.
 */
class CreditAuditCaApplMemberDeviceToOrientDBServiceImplTest extends AbstractRealTimeTest {

    @Resource
    private Sql groovySql

    @Test
    void testProcess() {
       /* String topic = 'credit_audit'

        Schema schema = new Schema.Parser().parse(kafkaDispatchConfig[topic]['ca_appl_member_device'].avroSchema)

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);

        ByteArrayOutputStream oos = new ByteArrayOutputStream()
        dataFileWriter.create(schema, oos)

        GenericRecord record = new GenericData.Record(schema)

        record.put('__schemaid__', '123456')
        record.put('APPL_NO', '1498060857052003')
        record.put('DEVICE_ID', 'ADFCA33B-BF8A-4FD8-937D-0F1A0ED4F2A9')
        record.put('IP', '117.136.67.71')
        record.put('IP_CITY', '苏州市')
        record.put('__op__', 'insert') //必须字段

        dataFileWriter.append(record)
        dataFileWriter.close()

        Producer<String, String> producer = new KafkaProducer<>(kafkaProducerProp)
        [0..10].each {
            producer.send(new ProducerRecord<String, Byte[]>(topic, 'ca_appl_member_device', oos.toByteArray()))
        }
        producer.close()*/

        List<Map> dataList = []
        groovySql.eachRow("select * from credit_audit.ca_appl_member_device where CREATE_TIME between '2017-06-29 00:00:00' and '2017-06-29 23:59:59'",
                {
                    row ->
                        dataList.add([
                                'APPL_NO'       : row.APPL_NO,
                                'DEVICE_ID'        : row.DEVICE_ID,
                                'IP'       : row.IP,
                                'IP_CITY': row.IP_CITY,
                                '___op___'        : 'insert'
                                ])
                }

        )
        produce('credit_audit', 'ca_appl_member_device', dataList)
    }
}
