package cn.memedai.orientdb.sns.realtime

import cn.memedai.orientdb.sns.realtime.batch.BatchService
import cn.memedai.orientdb.sns.realtime.teleporter.TeleporterService
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationContext
import org.springframework.context.support.ClassPathXmlApplicationContext
import org.springframework.stereotype.Component

/**
 * Created by kisho on 2017/6/8.
 */
@Component
class StartMain {

    static final Logger LOG = LoggerFactory.getLogger(StartMain.class)

    /**
     * 反欺诈业务实时数据同步脚本入口
     */
    static void main(args) {

        ApplicationContext context = null
        try {
            context = new ClassPathXmlApplicationContext('applicationContext.xml')

            //TODO test
            context.getBean(TeleporterService.class).process(null)
            context.getBean(BatchService.class).process(null)

//            KafkaConsumer consumer = new KafkaConsumer<>(context.getBean('kafkaProp'))
//            consumer.subscribe(context.getBean('kafkaTopics'))
//            while (true) {
//                ConsumerRecords records = null
//                try {
//                    records = consumer.poll(100)
//                    context.getBean(TeleporterService.class).process(records)
//                    context.getBean(BatchService.class).process(records)
//                } catch (RuntimeException e) {
//                    LOG.error("records is processed error : ${records}", e)
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
