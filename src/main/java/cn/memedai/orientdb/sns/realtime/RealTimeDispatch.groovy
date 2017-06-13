package cn.memedai.orientdb.sns.realtime

import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import groovy.json.JsonSlurper
import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.file.SeekableByteArrayInput
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumReader
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Service

import javax.annotation.PostConstruct
import javax.annotation.Resource

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class RealTimeDispatch {

    private static final LOG = LoggerFactory.getLogger(RealTimeDispatch.class)

    @Resource
    private ApplicationContext context

    @Resource
    private Properties defaultKafkaProp

    @Resource
    private Map<String, Map<String, String>> kafkaDispatchConfig

    private List<String> kafkaTopics = []

    private Map<String, DatumReader<GenericRecord>> topic2DatumReaderMap = [:]

    private Map<String, List<RealTimeService>> topic2ServicesMap = [:]

    private Map<String, Properties> topic2KafkaProMap = [:]

    void start() {
        //单线程跑
        KafkaConsumer consumer = new KafkaConsumer<>(defaultKafkaProp)
        consumer.subscribe(kafkaTopics)

        while (true) {
            ConsumerRecords records = consumer.poll(Long.MAX_VALUE)

            for (def topic in kafkaTopics) {
                Iterable iterable = records.records(topic)
                if (iterable == null || iterable.size() == 0) {
                    continue
                }

                //组装数据
                Iterator<ConsumerRecord> consumerRecordIterator = iterable.iterator()
                List<GenericRecord> recordList = []
                consumerRecordIterator.each {
                    DataFileReader<GenericRecord> dataReader = new DataFileReader<GenericRecord>(new SeekableByteArrayInput(it.value()), topic2DatumReaderMap[topic])
                    dataReader.each {
                        recordList.add(it)
                    }
                }

                def start = System.currentTimeMillis()
                def recordText = recordList.toString()
                //执行service
                topic2ServicesMap[topic].each {
                    it.process(new JsonSlurper().parseText(recordText))
                }
                LOG.info('topic->{},used time->{}ms,records->{}', topic, (System.currentTimeMillis() - start), recordText)
            }

            consumer.commitAsync()
        }
    }

    @PostConstruct
    void transfer() {
        def parser = new Schema.Parser()
        kafkaDispatchConfig.each {
            key, value ->
                kafkaTopics.add(key)

                topic2DatumReaderMap[key] = new GenericDatumReader<GenericRecord>(parser.parse(value.avroSchema))

                topic2ServicesMap[key] = []
                value.services.each {
                    topic2ServicesMap[key].add(it)
                }

                topic2KafkaProMap[key] = value.kafkaProp == null ? defaultKafkaProp : value.kafkaProp
        }
    }

}
