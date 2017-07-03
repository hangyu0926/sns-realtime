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
class CashLoanWalletMoneyBoxOrderToOrientDBServiceImplTest extends AbstractRealTimeTest{
  /*  @Resource
    private Properties kafkaProducerProp

    @Resource
    private Map<String, Map<String, Map<String, String>>> kafkaDispatchConfig*/

    @Resource
    private Sql groovySql

    @Test
    void testProcess() {
       /* String topic = 'cashloan'

        Schema schema = new Schema.Parser().parse(kafkaDispatchConfig[topic]['cash_loan_order'].avroSchema)

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);

        ByteArrayOutputStream oos = new ByteArrayOutputStream()
        dataFileWriter.create(schema, oos)

        GenericRecord record = new GenericData.Record(schema)

        record.put('__schemaid__', '123456')
        record.put('member_id', 840835L)
        record.put('order_no', '0320914418955835')
        record.put('phoneNo', '1')
        record.put('amount', 1000)
        record.put('status', '50')
        record.put('source', null)
        record.put('created_datetime', '2017-03-26 11:57:11')
        record.put('__op__', 'insert') //必须字段

        dataFileWriter.append(record)
        dataFileWriter.close()

        Producer<String, String> producer = new KafkaProducer<>(kafkaProducerProp)
        [0..10].each {
            producer.send(new ProducerRecord<String, Byte[]>(topic, 'cash_loan_order', oos.toByteArray()))
        }
        producer.close()*/
        List<Map> dataList = []
        groovySql.eachRow("select * from cashloan.cash_loan_order where created_datetime between '2017-06-29 00:00:00' and '2017-06-29 23:59:59'",
                {
                    row ->
                        dataList.add([
                                'member_id'       : row.member_id,
                                'order_no'        : row.order_no,
                                'phoneNo'       : row.phoneNo,
                                'amount': row.amount,
                                'status': row.status,
                                'created_datetime': row.created_datetime
                        ])
                }

        )
        produce('cashloan', 'apply_info', dataList)
    }
}
