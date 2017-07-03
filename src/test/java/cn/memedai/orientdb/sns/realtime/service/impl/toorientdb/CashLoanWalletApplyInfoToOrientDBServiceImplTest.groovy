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
 * Created by hangyu on 2017/6/20.
 */
class CashLoanWalletApplyInfoToOrientDBServiceImplTest extends AbstractRealTimeTest{

    @Resource
    private Sql groovySql

 /*   @Resource
    private Properties kafkaProducerProp

    @Resource
    private Map<String, Map<String, Map<String, String>>> kafkaDispatchConfig*/

    @Test
    void testProcess() {
       /* String topic = 'cashloan'

        Schema schema = new Schema.Parser().parse(kafkaDispatchConfig[topic]['apply_info'].avroSchema)

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);

        ByteArrayOutputStream oos = new ByteArrayOutputStream()
        dataFileWriter.create(schema, oos)

        GenericRecord record = new GenericData.Record(schema)

        record.put('__schemaid__', '123456')
        record.put('member_id', 1529357L)
        record.put('apply_no', '0326063174631357')
        record.put('cellphone', '1')
        record.put('source', '0')
        record.put('created_datetime', '2017-03-26 11:57:11')
        record.put('ip1', '2')
        record.put('ip1_city', '银川市')
        record.put('device_id', '477FF9FD-0D4D-425A-9909-0944B0159D69')
        record.put('__op__', 'insert') //必须字段

        dataFileWriter.append(record)
        dataFileWriter.close()

        Producer<String, String> producer = new KafkaProducer<>(kafkaProducerProp)
        [0..10].each {
            producer.send(new ProducerRecord<String, Byte[]>(topic, 'apply_info', oos.toByteArray()))
        }
        producer.close()*/

        List<Map> dataList = []
        groovySql.eachRow("select * from cashloan.apply_info where created_datetime between '2017-06-29 00:00:00' and '2017-06-29 23:59:59'",
                {
                    row ->
                        dataList.add([
                                'member_id'       : row.member_id,
                                'apply_no'        : row.apply_no,
                                'cellphone'       : row.cellphone,
                                'source': row.source,
                                'created_datetime': row.created_datetime,
                                'ip1': row.ip1,
                                'ip1_city': row.ip1_city,
                                'device_id': row.device_id
                        ])
                }

        )
        produce('cashloan', 'apply_info', dataList)
    }
}
