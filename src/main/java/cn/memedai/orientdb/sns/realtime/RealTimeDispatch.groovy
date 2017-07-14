package cn.memedai.orientdb.sns.realtime

import cn.memedai.orientdb.sns.realtime.service.MailService
import groovy.json.JsonSlurper
import groovy.sql.Sql
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.BinaryDecoder
import org.apache.avro.io.DecoderFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

import javax.annotation.Resource
import java.util.concurrent.ExecutorService
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class RealTimeDispatch {

    private static final LOG = LoggerFactory.getLogger(RealTimeDispatch.class)

    @Resource
    private ExecutorService executorService

    @Resource
    private Properties kafkaProp

    @Resource
    private KafkaConfig kafkaConfig

    @Resource
    private Sql sql

    @Resource
    private MailService mailService

    private ReadWriteLock lock = new ReentrantReadWriteLock()

    private List<Throwable> throwables = Collections.synchronizedList([])

    void start() {
        List<Future> futures = []
        kafkaConfig.topics.each {
            topic ->
                String currentTopic = topic.substring(topic.lastIndexOf('.') + 1)
                Integer threadCount = kafkaConfig.topic2ThreadCountMap[currentTopic]
                threadCount.times {
                    futures.add(executorService.submit({
                        try {
                            startThread(topic)
                        } catch (org.apache.kafka.common.errors.InterruptException ignoreException) {
                            LOG.warn('{} is interrupted caused by {}', Thread.currentThread(), ignoreException.toString())
                            executorService.shutdownNow()
                        } catch (Throwable e) {
                            addThrowables(e)
                        }
                    }))
                }
        }

        futures.each {
            try {
                it.get()
            } catch (Throwable e) {

            }
        }

        LOG.info("total statistics->${kafkaConfig.topic2ProcessedStatisticsMap}")

        if (!throwables.isEmpty()) {
            mailService.sendMail(throwables)
        }
    }

    private void startThread(final String topic) {
        final KafkaConsumer consumer = new KafkaConsumer<>(kafkaProp)
        consumer.subscribe([topic])
        String currentTopic = topic.substring(topic.lastIndexOf('.') + 1)
        LOG.info("Subscribed the topic {} successfully!", topic)
        while (true) {
            pollAndProcess(currentTopic, consumer)
        }
    }

    private void pollAndProcess(String topic, KafkaConsumer consumer) {
        final ConsumerRecords records = consumer.poll(Long.MAX_VALUE)
        kafkaConfig.topic2ProcessedStatisticsMap[topic]['poll']?.getAndIncrement()
        Map<String, Object> thisStatistics = ['records': records.size(), 'insert': new AtomicLong(0), 'update': new AtomicLong()]
        long start = System.currentTimeMillis()
        JsonSlurper jsonSlurper = new JsonSlurper()
        records.each {
            record ->
                //topic：credit_audit2更好只有一个table，且key为applyNo，此处需要特殊处理下.
                String table = topic == 'credit_audit2' ? 'ca_bur_operator_contact' : record.key().toString().toLowerCase()
                String dbtable = "$topic${table}"
                if (!kafkaConfig.dbtable2DatumReaderMap.containsKey(dbtable)) {
                    LOG.debug("ignore:topic->$topic,key->${table}")
                    return
                }
                String dataListText = null
                try {
                    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(record.value(), null)
                    GenericRecord avroGenericRecord = kafkaConfig.dbtable2DatumReaderMap[dbtable].read(null, decoder)

                    dataListText = [avroGenericRecord.toString()].toString()
                    def dataList = jsonSlurper.parseText(dataListText)
                    dataList.each {
                        it['___genericRecord___'] = avroGenericRecord
                    }
                    kafkaConfig.topic2ProcessedStatisticsMap[topic][dataList[0].___op___]?.getAndIncrement()
                    thisStatistics[dataList[0].___op___]?.getAndIncrement()
                    //执行service
                    kafkaConfig.dbtable2ServicesMap[dbtable].each {
                        service ->
                            long start1 = System.currentTimeMillis()
                            service.process(dataList)
                            getLogger(topic).info('process class->{},this statistics->{},topic statistics->{},used time->{}ms', service.getClass().getSimpleName(), thisStatistics, kafkaConfig.topic2ProcessedStatisticsMap[topic], (System.currentTimeMillis() - start1))
                    }
                } catch (Throwable e) {
                    getLogger(topic).warn("record.value()->${record.value()}")
                    getLogger(topic).warn('consume result->{},topic->{},key->{},this statistics->{},topic statistics->{},used time->{}ms,record->{}', 'fail', topic, table, thisStatistics, kafkaConfig.topic2ProcessedStatisticsMap[topic], (System.currentTimeMillis() - start), dataListText)
                    throw e
                }
        }

        try {
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition)
                long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset()
                consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)))
            }
            getLogger(topic).info('consume result->{},topic->{},this statistics->{},topic statistics->{},used time->{}ms', 'success', topic, thisStatistics, kafkaConfig.topic2ProcessedStatisticsMap[topic], (System.currentTimeMillis() - start))
        } catch (Exception e) {
            LOG.error("ignore this exception", e)
        }
    }

    private Logger getLogger(String topic) {
        kafkaConfig.topic2LoggerMap[topic]
    }

    private void addThrowables(Throwable e) {
        LOG.error('', e)
        lock.writeLock().lock()
        try {
            throwables.add(e)
            executorService.shutdownNow()
        } finally {
            lock.writeLock().unlock()
        }
    }

}
