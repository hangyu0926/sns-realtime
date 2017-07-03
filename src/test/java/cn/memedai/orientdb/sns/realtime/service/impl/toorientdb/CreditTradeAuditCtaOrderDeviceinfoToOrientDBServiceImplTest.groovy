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
class CreditTradeAuditCtaOrderDeviceinfoToOrientDBServiceImplTest extends AbstractRealTimeTest{

    @Resource
    private Sql groovySql

  /*  @Resource
    private Properties kafkaProducerProp

    @Resource
    private Map<String, Map<String,  Map<String, String>>> kafkaDispatchConfig*/

    @Test
    void testProcess() {
       /* String topic = 'credit_trade_audit'

        Schema schema = new Schema.Parser().parse(kafkaDispatchConfig[topic]['cta_order_deviceinfo'].avroSchema)

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);

        ByteArrayOutputStream oos = new ByteArrayOutputStream()
        dataFileWriter.create(schema, oos)

        GenericRecord record = new GenericData.Record(schema)

        record.put('__schemaid__', '123456')
        record.put('ORDER_ID', '1486790611282001')
        record.put('DEVICE_ID', '869949026162329')
        record.put('IP', '116.228.236.198')
        record.put('IP_CITY', '上海市')
        record.put('__op__', 'insert') //必须字段

        dataFileWriter.append(record)
        dataFileWriter.close()

        Producer<String, String> producer = new KafkaProducer<>(kafkaProducerProp)
        [0..10].each {
            producer.send(new ProducerRecord<String, Byte[]>(topic, 'cta_order_deviceinfo', oos.toByteArray()))
        }
        producer.close()*/

        List<Map> dataList = []
        groovySql.eachRow("select * from credit_trade_audit.cta_order_deviceinfo where CREATE_TIME between '2017-06-29 00:00:00' and '2017-06-29 23:59:59'",
                {
                    row ->
                        dataList.add([
                                'ORDER_ID'       : row.ORDER_ID,
                                'DEVICE_ID'        : row.DEVICE_ID,
                                'IP'       : row.IP,
                                'IP_CITY': row.IP_CITY,
                                '___op___'        : 'insert'
                        ])
                }

        )
        produce('credit_audit', 'cta_order_deviceinfo', dataList)
    }
}
