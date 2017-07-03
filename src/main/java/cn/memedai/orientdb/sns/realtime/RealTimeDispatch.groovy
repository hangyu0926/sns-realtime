package cn.memedai.orientdb.sns.realtime

import cn.memedai.orientdb.sns.realtime.service.MailService
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import groovy.json.JsonSlurper
import groovy.sql.Sql
import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.file.SeekableByteArrayInput
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumReader
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
import java.util.concurrent.ExecutorService
import java.util.concurrent.Future
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

    private List<Future> futures = Collections.synchronizedList([])

    private Set<String> topics = []

    private Map<String, DatumReader<GenericRecord>> dbtable2DatumReaderMap = [:]

    void start() {
        topics.each {
            topic ->
                futures.add(executorService.submit({
                    try {
                        startThread(topic)
                    } catch (org.apache.kafka.common.errors.InterruptException ignoreException) {

                    } catch (Throwable e) {
                        addThrowables(e)
                    }
                }))
        }

        futures.each {
            try {
                it.get()
            } catch (Throwable e) {
                executorService.shutdownNow()
            }
        }

        throwables.each {
            LOG.error('', it)
        }

        if (!throwables.isEmpty()) {
            mailService.sendMail(throwables)
        }
    }

    private Map<String, List<RealTimeService>> dbtable2ServicesMap = [:]

    private Map<String, ExecutorService> topic2ThreadPoolMap = [:]

    private Map<String, Logger> topic2LoggerMap = [:]

    private void startThread(final String topic) {
        final KafkaConsumer consumer = new KafkaConsumer<>(kafkaProp)
        consumer.subscribe([topic])
        LOG.info("Subscribed the topic {} successfully!", topic)
        while (true && !isTerminated()) {
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

    private void pollAndProcess(String topic, KafkaConsumer consumer) {
        final ConsumerRecords records = consumer.poll(Long.MAX_VALUE)
        if (records.size() > 1) {
            getLogger(topic).info('records size->{}', records.size())
        }
        JsonSlurper jsonSlurper = new JsonSlurper()
        records.each {
            record ->
                DataFileReader<GenericRecord> dataReader = null
                String dbtable = "${topic}${record.key()}"
                try {
                    dataReader = new DataFileReader<GenericRecord>(new SeekableByteArrayInput(record.value()), dbtable2DatumReaderMap[dbtable])
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
                            dbtable2ServicesMap[dbtable].each {
                                service ->
                                    long start2 = System.currentTimeMillis()
                                    service.process(dataList)
                                    getLogger(topic).info('{}#process, used time->{}ms', service.getClass().getSimpleName(), (System.currentTimeMillis() - start2))
                            }
                            getLogger(topic).info('consumer result->{},topic->{},table->{},used time->{}ms', 'success', topic, record.key(), (System.currentTimeMillis() - start))
                        } catch (Throwable e) {
                            getLogger(topic).warn('consume result->{},topic->{},table-{},used time->{}ms,record->{}', 'fail', topic, record.key(), (System.currentTimeMillis() - start), dateListText)
                            throw e
                        }
                }
        }
        commitAsync(consumer, records)
    }

    private void commitAsync(KafkaConsumer consumer, ConsumerRecords records) {
        try {
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition)
                long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset()
                consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)))
            }
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

                topics.add(topic)
                dbtable2DatumReaderMap[dbtable] = new GenericDatumReader<GenericRecord>(parser.parse(avscFile.text))
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

    private boolean isTerminated() {
        lock.readLock().lock()
        try {
            return !throwables.isEmpty()
        } finally {
            lock.readLock().unlock()
        }
    }

    private void addThrowables(Throwable e) {
        lock.writeLock().lock()
        try {
            throwables.add(e)
            futures.each {
                it.cancel(true)
            }
            executorService.shutdownNow()
        } finally {
            lock.writeLock().unlock()
        }
    }

}
