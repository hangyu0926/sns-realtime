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
import org.apache.commons.lang.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

import javax.annotation.PostConstruct
import javax.annotation.Resource
import java.util.concurrent.ExecutorService
import java.util.concurrent.Future

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

    @Resource
    private Map<String, ExecutorService> topicDispatchMap

    private Map<String, DatumReader<GenericRecord>> table2DatumReaderMap = [:]

    private Map<String, List<RealTimeService>> table2ServicesMap = [:]

    private Map<String, Properties> topic2KafkaProMap = [:]

    private Map<String, Logger> topic2LoggerMap = [:]

    @Resource
    private Sql sql

    void start() {
        List<Future> futures = []
        kafkaDispatchConfig.keySet().each {
            futures.add(executorService.submit(new Runnable() {
                @Override
                void run() {
                    startConsumerThread(it)
                }
            }))
        }
        futures.each {
            it.get()
        }
    }

    private void startConsumerThread(final String topic) {
        final KafkaConsumer consumer = new KafkaConsumer<>(topic2KafkaProMap[topic])
        consumer.subscribe([topic])
        LOG.info("Subscribed the topic {} successfully!", topic)
        while (true) {
            try {
                final ConsumerRecords records = consumer.poll(Long.MAX_VALUE)
                if (records.size() > 1) {
                    LOG.info('records size->{}', records.size())
                }
                ExecutorService subExecutorService = topicDispatchMap[topic]
                if (subExecutorService == null) {
                    startSubConsumer(topic, consumer, records)
                } else {
                    subExecutorService.submit(new Runnable() {
                        @Override
                        void run() {
                            startSubConsumer(topic, consumer, records)
                        }
                    })
                }
            } catch (Throwable e) {
                LOG.error("", e)
                throw e
            }
        }

    }

    private void startSubConsumer(String topic, KafkaConsumer consumer, ConsumerRecords records) {
        JsonSlurper jsonSlurper = new JsonSlurper()
        records.each {
            record ->
                DataFileReader<GenericRecord> dataReader = null
                try {
                    dataReader = new DataFileReader<GenericRecord>(new SeekableByteArrayInput(record.value()), table2DatumReaderMap[record.key()])
                } catch (IOException e) {
                    getLogger(topic).error('schema does not match!', e)
                    getLogger(topic).warn('consume result->{},records->{}', 'fail', records.asCollection().toString())
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
                                service ->
                                    long start2 = System.currentTimeMillis()
                                    service.process(dataList)
                                    getLogger(topic).info('{}#process, used time->{}ms', service.getClass().getSimpleName(), (System.currentTimeMillis() - start2))
                            }
                            getLogger(topic).info('consumer result->{},topic->{},table->{},used time->{}ms', 'success', topic, record.key(), (System.currentTimeMillis() - start))
                        } catch (Throwable e) {
                            getLogger(topic).error("", e)
                            getLogger(topic).warn('consume result->{},topic->{},table-{},used time->{}ms,record->{}', 'fail', topic, record.key(), (System.currentTimeMillis() - start), dateListText)
                            throw e
                        }
                }
        }
        commitAsync(consumer, records)
    }

    private void commitAsync(KafkaConsumer consumer, ConsumerRecords records) {
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, String>> partitionRecords = records.records(partition)
            long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset()
            consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)))
        }
    }

    @PostConstruct
    private void transfer() {
        def parser = new Schema.Parser()
        JsonSlurper jsonSlurper = new JsonSlurper()
        Map table2SchemaMap = [:]
        new File("${getClass().getResource('/').toString()}avsc".replaceFirst('file:', '')).listFiles().each {
            Map schemaMap = jsonSlurper.parseText(it.text)
            String table = "${schemaMap.namespace.substring(schemaMap.namespace.lastIndexOf('.') + 1)}.${schemaMap.name}"
            table2SchemaMap[table] = it.text
        }
        kafkaDispatchConfig.each {
            topic, tableConfigInTopic ->
                tableConfigInTopic.each {
                    table, tableConfig ->
                        String avroSchema = tableConfig.avroSchema
                        if (StringUtils.isEmpty(avroSchema)) {
                            avroSchema = table2SchemaMap["${topic}.${table}"]
                        }
                        table2DatumReaderMap[table] = new GenericDatumReader<GenericRecord>(parser.parse(avroSchema))
                        table2ServicesMap[table] = tableConfig.services
                }
                Properties kafkaProp = new Properties()
                kafkaProp.putAll(kafkaConfigMap[topic] == null ? defaultKafkaMap : kafkaConfigMap[topic])
                topic2KafkaProMap[topic] = kafkaProp
                topic2KafkaProMap[topic]['group.id'] = 'sns_' + topic
                topic2LoggerMap[topic] = LoggerFactory.getLogger("${RealTimeDispatch.class.getName()}#$topic")
        }
    }

    private Logger getLogger(String topic) {
        topic2LoggerMap[topic]
    }

}
