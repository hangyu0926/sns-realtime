package cn.memedai.orientdb.sns.realtime

import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import groovy.json.JsonSlurper
import groovy.sql.Sql
import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.file.SeekableByteArrayInput
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumReader
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

import javax.annotation.PostConstruct
import javax.annotation.Resource
import java.util.concurrent.ExecutorService

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class RealTimeDispatch {

    private static final LOG = LoggerFactory.getLogger(RealTimeDispatch.class)

    @Resource
    private ExecutorService executorService

    @Resource
    private Map defaultKafkaMap

    @Resource
    private Map<String, Map> kafkaConfigMap

    @Resource
    private Map<String, Map<String, Map<String, Object>>> kafkaDispatchConfig

    private Map<String, DatumReader<GenericRecord>> table2DatumReaderMap = [:]

    private Map<String, List<RealTimeService>> table2ServicesMap = [:]

    private Map<String, Properties> topic2KafkaProMap = [:]

    @Resource
    private Sql sql

    void start() {
        kafkaDispatchConfig.keySet().each {
            executorService.submit(new Runnable() {
                @Override
                void run() {
                    startConsumerThread(it)
                }
            })
        }
    }

    private void startConsumerThread(String topic) {
        JsonSlurper jsonSlurper = new JsonSlurper()
        Logger cLog = LoggerFactory.getLogger("${getClass().getName()}#$topic")
        KafkaConsumer consumer = new KafkaConsumer<>(topic2KafkaProMap[topic])
        consumer.subscribe([topic])
        cLog.info("Subscribed the topic {} successfully!", topic)
        while (true) {
            try {
                ConsumerRecords records = consumer.poll(Long.MAX_VALUE)
                records.each {
                    record ->
                        DataFileReader<GenericRecord> dataReader = null
                        try {
                            dataReader = new DataFileReader<GenericRecord>(new SeekableByteArrayInput(record.value()), table2DatumReaderMap[record.key()])
                        } catch (IOException e) {
                            cLog.error('schema does not match!', e)
                            cLog.warn('consume result->{},records->{}', 'fail', records.asCollection().toString())
                            return
                        }
                        dataReader.each {
                            data ->
                                long start = System.currentTimeMillis()
                                def dateListText = [data.toString()].toString()
                                def dataList = jsonSlurper.parseText(dateListText)
                                try {
                                    //执行service
                                    table2ServicesMap[record.key()].each {
                                        it.process(dataList)
                                    }
                                    cLog.info('consumer result->{},topic->{},table-{},used time->{}ms', 'success', topic, record.key(), (System.currentTimeMillis() - start))
                                } catch (Throwable e) {
                                    cLog.error("", e)
                                    cLog.warn('consume result->{},topic->{},table-{},used time->{}ms,record->{}', 'fail', topic, record.key(), (System.currentTimeMillis() - start), dateListText)
                                    throw e
                                }
                        }
                }
                consumer.commitAsync()
            } catch (Throwable e) {
                LOG.error("", e)
                throw e
            }
        }

    }

    @PostConstruct
    private void transfer() {
        def parser = new Schema.Parser()
        kafkaDispatchConfig.each {
            topic, tableConfigInTopic ->
                tableConfigInTopic.each {
                    table, tableConfig ->
                        table2DatumReaderMap[table] = new GenericDatumReader<GenericRecord>(parser.parse(tableConfig.avroSchema))
                        table2ServicesMap[table] = tableConfig.services
                }
                Properties kafkaProp = new Properties()
                kafkaProp.putAll(kafkaConfigMap[topic] == null ? defaultKafkaMap : kafkaConfigMap[topic])
                topic2KafkaProMap[topic] = kafkaProp
                topic2KafkaProMap[topic]['group.id'] = 'sns_' + topic
        }
    }

}
