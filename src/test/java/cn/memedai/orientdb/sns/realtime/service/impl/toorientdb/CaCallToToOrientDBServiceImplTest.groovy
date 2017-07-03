package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

import cn.memedai.orientdb.sns.realtime.AbstractRealTimeTest
import groovy.sql.Sql
import org.junit.Test

import javax.annotation.Resource

/**
 * Created by hangyu on 2017/6/14.
 */
class CaCallToToOrientDBServiceImplTest extends AbstractRealTimeTest {

    @Resource
    private Sql sql

    @Test
    void testProcess() {
       /* String topic = 'credit_audit'

        Schema schema = new Schema.Parser().parse(kafkaDispatchConfig[topic]['ca_bur_operator_contact'].avroSchema)

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);

        ByteArrayOutputStream oos = new ByteArrayOutputStream()
        dataFileWriter.create(schema, oos)

        GenericRecord record = new GenericData.Record(schema)

        record.put('__schemaid__', '123456')
         //record.put('APPL_NO', '1497885791454002')
        record.put('APPL_NO', '1498578768406002')
        //record.put('APPL_NO', '1498578818266000')
        //record.put('APPL_NO', '1498578833840002')
        //record.put('APPL_NO', '1498578898305001')
        //record.put('APPL_NO', '1498579227034003')
        record.put('PHONE_NO', '1')
        record.put('CALL_CNT', 0)
        record.put('CALL_LEN', 0)
        record.put('CALL_IN_CNT', 0)
        record.put('CALL_OUT_CNT', 0)
        record.put('CREATE_TIME', '2017-06-19 23:57:10')
        record.put('__op__', 'insert') //必须字段

        dataFileWriter.append(record)
        dataFileWriter.close()

        Producer<String, String> producer = new KafkaProducer<>(kafkaProducerProp)
        [0..10].each {
            producer.send(new ProducerRecord<String, Byte[]>(topic, 'ca_bur_operator_contact', oos.toByteArray()))
        }
        producer.close()*/

        def list = []
        sql.rows("select APPL_NO,PHONE_NO,CALL_CNT,CALL_LEN,CALL_IN_CNT,CALL_OUT_CNT,CREATE_TIME from credit_audit.ca_bur_operator_contact where id > 43611474 and  PHONE_NO is not null limit 50000").each {
            row ->
                list.add(row.APPL_NO)
        }


        for (i in list)
        produce('credit_audit', 'ca_bur_operator_contact', [[
                                                 'APPL_NO'       : i,
                                                 'PHONE_NO'        : '1',
                                                 'CALL_CNT'       : 0,
                                                 'CALL_IN_CNT': 0,
                                                 'CALL_OUT_CNT'    : 0,
                                                 'CREATE_TIME'        : '2017-06-19 23:57:10'
                                         ]])
    }
}
