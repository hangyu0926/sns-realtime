package cn.memedai.orientdb.sns.realtime

import cn.memedai.orientdb.sns.realtime.service.MailService
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import groovy.json.JsonSlurper
import groovy.sql.Sql
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.BinaryDecoder
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader
import org.apache.commons.collections.CollectionUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Service

import javax.annotation.PostConstruct
import javax.annotation.Resource
import java.util.concurrent.ConcurrentHashMap
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
    private ApplicationContext context

    @Resource
    private ExecutorService executorService

    @Resource
    private Properties kafkaProp

    @Resource
    private Map kafkaDispatchConfig

    @Resource
    private Sql sql

    @Resource
    private MailService mailService

    private ReadWriteLock lock = new ReentrantReadWriteLock()

    private List<Throwable> throwables = Collections.synchronizedList([])

    private Set<String> topics = []

    private Map<String, DatumReader> dbtable2DatumReaderMap = [:]

    private Map<String, List<RealTimeService>> dbtable2ServicesMap = [:]

    private Map<String, ExecutorService> topic2ThreadPoolMap = [:]

    private Map<String, Logger> topic2LoggerMap = [:]

    private Map<String, Map<String, AtomicLong>> topic2ProcessedStatisticsMap = new ConcurrentHashMap<>()

    void start() {
        List<Future> futures = []
        topics.each {
            topic ->
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

        futures.each {
            it.get()
        }

        throwables.each {
            LOG.error('', it)
        }

        LOG.info("total statistics->$topic2ProcessedStatisticsMap")

        if (!throwables.isEmpty()) {
            mailService.sendMail(throwables)
        }
    }

    private void startThread(final String topic) {
        final KafkaConsumer consumer = new KafkaConsumer<>(kafkaProp)
        consumer.subscribe([topic])
        LOG.info("Subscribed the topic {} successfully!", topic)
        while (true) {
            ExecutorService subExecutorService = topic2ThreadPoolMap[topic]
            if (subExecutorService == null) {
                pollAndProcess(topic, consumer)
            } else {
                startSubThread(topic, consumer)
            }
        }
    }

    private void startSubThread(String topic, KafkaConsumer consumer) {
        topic2ThreadPoolMap[topic].submit({
            pollAndProcess(topic, consumer)
        })
    }

    private void pollAndProcess(String originalTopic, KafkaConsumer consumer) {
        String topic = originalTopic.substring(originalTopic.lastIndexOf('.') + 1)
        final ConsumerRecords records = consumer.poll(Long.MAX_VALUE)
        topic2ProcessedStatisticsMap[topic]['poll']?.getAndIncrement()
        Map<String, Object> thisStatistics = ['records': records.size(), 'insert': new AtomicLong(0), 'update': new AtomicLong()]
        long start = System.currentTimeMillis()
        JsonSlurper jsonSlurper = new JsonSlurper()
        records.each {
            record ->
                String dbtable = "$topic${record.key()}"
                if (!dbtable2DatumReaderMap.containsKey(dbtable)) {
                    LOG.debug("ignore:topic->$originalTopic,table->${record.key()}")
                    return
                }
                String dataListText = null
                try {
                    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(record.value(), null)
                    GenericRecord avroGenericRecord = dbtable2DatumReaderMap[dbtable].read(null, decoder)

                    dataListText = [avroGenericRecord.toString()].toString()
                    def dataList = jsonSlurper.parseText(dataListText)

                    topic2ProcessedStatisticsMap[topic][dataList[0].___op___]?.getAndIncrement()
                    thisStatistics[dataList[0].___op___]?.getAndIncrement()
                    //执行service
                    dbtable2ServicesMap[dbtable].each {
                        service ->
                            long start1 = System.currentTimeMillis()
                            service.process(dataList)
                            getLogger(topic).info('process class->{},this statistics->{},topic statistics->{},used time->{}ms', service.getClass().getSimpleName(), thisStatistics, topic2ProcessedStatisticsMap[topic], (System.currentTimeMillis() - start1))
                    }
                } catch (Throwable e) {
                    getLogger(topic).warn('consume result->{},topic->{},table->{},this statistics->{},topic statistics->{},used time->{}ms,record->{}', 'fail', topic, record.key(), thisStatistics, topic2ProcessedStatisticsMap[topic], (System.currentTimeMillis() - start), dataListText)
                    throw e
                }
        }

        try {
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition)
                long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset()
                consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)))
            }
            getLogger(topic).info('consume result->{},topic->{},this statistics->{},topic statistics->{},used time->{}ms', 'success', topic, thisStatistics, topic2ProcessedStatisticsMap[topic], (System.currentTimeMillis() - start))
        } catch (Exception e) {
            LOG.error("ignore this exception", e)
        }
    }


    @PostConstruct
    private void init() {
        def parser = new Schema.Parser()
        JsonSlurper jsonSlurper = new JsonSlurper()
        new File("${getClass().getResource('/').toString()}avsc".replaceFirst('file:', '')).listFiles().each {
            avscFile ->
                Map schemaMap = jsonSlurper.parseText(avscFile.text)
                String topic = schemaMap.namespace.substring(schemaMap.namespace.lastIndexOf('.') + 1)
                String table = schemaMap.name
                String dbtable = "$topic$table"

                topics.add(schemaMap.namespace)
                dbtable2DatumReaderMap[dbtable] = new SpecificDatumReader(parser.parse(avscFile.text))
                topic2ThreadPoolMap[topic] = kafkaDispatchConfig."$topic"?.threadPool
                List<RealTimeService> services = kafkaDispatchConfig."$topic"?.tableConfig?.services
                if (CollectionUtils.isEmpty(services)) {
                    services = []
                    String beanNamePrefix = "${getUpperCamelCaseWord(topic)}${getUpperCamelCaseWord(table)}"
                    beanNamePrefix = "${beanNamePrefix.substring(0, 1).toLowerCase()}${beanNamePrefix.substring(1)}"
                    String toOrientDbBean = "${beanNamePrefix}ToOrientDBServiceImpl"
                    if (context.containsBean(toOrientDbBean)) {
                        services.add(context.getBean(toOrientDbBean))
                    }
                    String toMysqlBean = "${beanNamePrefix}ToMysqlServiceImpl"
                    if (context.containsBean(toMysqlBean)) {
                        services.add(context.getBean(toMysqlBean))
                    }
                    dbtable2ServicesMap[dbtable] = services
                }
                topic2LoggerMap[topic] = LoggerFactory.getLogger("${RealTimeDispatch.class.getName()}#$topic")

                topic2ProcessedStatisticsMap[topic] = ['insert': new AtomicLong(0), 'update': new AtomicLong(0), 'poll': new AtomicLong(0)]
        }

        LOG.info('********************************init info start********************************')
        LOG.info("topics->$topics")
        LOG.info("dbtable2DatumReaderMap->$dbtable2DatumReaderMap")
        LOG.info("topic2ThreadPoolMap->$topic2ThreadPoolMap")
        LOG.info("dbtable2ServicesMap->$dbtable2ServicesMap")
        LOG.info("topic2LoggerMap->$topic2LoggerMap")
        LOG.info('********************************init info end********************************')
    }

    private Logger getLogger(String topic) {
        topic2LoggerMap[topic]
    }

    private String getUpperCamelCaseWord(String s) {
        StringBuilder builder = new StringBuilder()
        s.split("_").each {
            String tempStr = it.toLowerCase()
            builder.append(tempStr.substring(0, 1).toUpperCase()).append(tempStr.substring(1))
        }
        builder.toString()
    }

    private void addThrowables(Throwable e) {
        lock.writeLock().lock()
        try {
            throwables.add(e)
            executorService.shutdownNow()
        } finally {
            lock.writeLock().unlock()
        }
    }

}
