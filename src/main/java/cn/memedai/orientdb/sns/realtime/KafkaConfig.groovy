package cn.memedai.orientdb.sns.realtime

import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import groovy.json.JsonSlurper
import org.apache.avro.Schema
import org.apache.avro.io.DatumReader
import org.apache.avro.specific.SpecificDatumReader
import org.apache.commons.collections.CollectionUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Service

import javax.annotation.PostConstruct
import javax.annotation.Resource
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class KafkaConfig {

    private static final LOG = LoggerFactory.getLogger(KafkaConfig.class)

    @Resource
    private ApplicationContext context

    @Resource
    private Map kafkaDispatchConfig

    Set<String> topics = []

    Map<String, DatumReader> dbtable2DatumReaderMap = new ConcurrentHashMap<>()

    Map<String, Schema> dbtable2SchemaMap = new ConcurrentHashMap<>()

    Map<String, Integer> topic2ThreadCountMap = new ConcurrentHashMap<>()

    Map<String, List<RealTimeService>> dbtable2ServicesMap = new ConcurrentHashMap<>()

    Map<String, Logger> topic2LoggerMap = new ConcurrentHashMap<>()

    Map<String, Map<String, AtomicLong>> topic2ProcessedStatisticsMap = new ConcurrentHashMap<>()

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
                Schema schema = parser.parse(avscFile.text)
                dbtable2SchemaMap[dbtable] = schema
                dbtable2DatumReaderMap[dbtable] = new SpecificDatumReader(schema)
                List<RealTimeService> services = kafkaDispatchConfig."$topic"?.tableConfig?."$table"?.services
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
                }
                dbtable2ServicesMap[dbtable] = services
        }

        topics.each {
            String topic = it.substring(it.lastIndexOf('.') + 1)
            topic2LoggerMap[topic] = LoggerFactory.getLogger("${RealTimeDispatch.class.getName()}#$topic")

            topic2ProcessedStatisticsMap[topic] = ['insert': new AtomicLong(0), 'update': new AtomicLong(0), 'poll': new AtomicLong(0)]
            String threadCount = kafkaDispatchConfig."$topic"?.threadCount
            topic2ThreadCountMap[topic] = (threadCount == null || threadCount <= 0) ? 1 : Integer.parseInt(threadCount.toString())
        }
        LOG.info('********************************init info start********************************')
        LOG.info("topics->$topics")
        LOG.info("topic2ThreadCountMap->$topic2ThreadCountMap")
        LOG.info("topic2LoggerMap->$topic2LoggerMap")
        LOG.info("dbtable2DatumReaderMap->$dbtable2DatumReaderMap")
        LOG.info("dbtable2SchemaMap->$dbtable2SchemaMap")
        LOG.info("dbtable2ServicesMap->$dbtable2ServicesMap")
        LOG.info('********************************init info end********************************')
    }

    private String getUpperCamelCaseWord(String s) {
        StringBuilder builder = new StringBuilder()
        s.split("_").each {
            String tempStr = it.toLowerCase()
            builder.append(tempStr.substring(0, 1).toUpperCase()).append(tempStr.substring(1))
        }
        builder.toString()
    }

}
