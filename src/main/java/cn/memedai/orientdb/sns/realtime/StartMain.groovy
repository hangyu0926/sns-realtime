package cn.memedai.orientdb.sns.realtime

import cn.memedai.orientdb.sns.realtime.batch.BatchService
import cn.memedai.orientdb.sns.realtime.teleporter.TeleporterService
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationContext
import org.springframework.context.support.ClassPathXmlApplicationContext
import org.springframework.stereotype.Component

import javax.annotation.PostConstruct
import javax.annotation.Resource

/**
 * Created by kisho on 2017/6/8.
 */
@Component
class StartMain {

    static final Logger LOG = LoggerFactory.getLogger(StartMain.class)

    private static StartMain INSTANCE

    @Resource
    private TeleporterService teleporterService

    @Resource
    private BatchService batchService

    @Resource
    private Properties kafkaProp

    @Resource
    private List<String> kafkaTopics

    @PostConstruct
    private void postConstruct() {
        INSTANCE = this
    }
    /**
     * 反欺诈业务实时数据同步脚本入口
     */
    static void main(String[] args) {

        ApplicationContext context = null
        try {
            context = new ClassPathXmlApplicationContext('applicationContext.xml')

            //TODO test
            LOG.info(null)
            INSTANCE.teleporterService.process(null)
            INSTANCE.batchService.process(null)

//            KafkaConsumer consumer = new KafkaConsumer<>(INSTANCE.kafkaProp)
//            consumer.subscribe(INSTANCE.kafkaTopics)
//            while (true) {
//                ConsumerRecords<String, String> records = consumer.poll(100)
//                records.each {
//                    LOG.info(it)
//                    teleporterService.process(it)
//                    batchService.process(it)
//                }
//            }

        } catch (Throwable e) {
            LOG.error("", e)
        } finally {
            if (context != null) {
                context.close()
            }
        }
    }

}
