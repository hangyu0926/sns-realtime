package cn.memedai.orientdb.sns.realtime

import cn.memedai.orientdb.sns.realtime.batch.BatchService
import cn.memedai.orientdb.sns.realtime.teleporter.TeleporterService
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationContext
import org.springframework.context.support.ClassPathXmlApplicationContext

/**
 * Created by kisho on 2017/6/8.
 */
class RealTimeMain {
    private static final Logger LOG = LoggerFactory.getLogger(RealTimeMain.class)

    static void main(args) {
        /**
         * 反欺诈业务实时数据同步脚本入口
         */
        ApplicationContext context = null
        KafkaConsumer consumer = null
        try {
            context = new ClassPathXmlApplicationContext('applicationContext.xml')

            //TODO test
            context.getBean(TeleporterService.class).process(null)
            context.getBean(BatchService.class).process(null)

            consumer = new KafkaConsumer<>(context.getBean('kafkaProp'))
            consumer.subscribe(context.getBean('kafkaTopics'))
            while (true) {
                ConsumerRecords records = null
                try {
                    records = consumer.poll(Long)
                    context.getBean(TeleporterService.class).process(records)
                    context.getBean(BatchService.class).process(records)

                    consumer.commitAsync()
                } catch (RuntimeException e) {
                    LOG.error("records is processed error : ${records}", e)
                }
            }

        } catch (Throwable e) {
            LOG.error("", e)
        } finally {
            if (context != null) {
                context.close()
            }
            if (consumer != null) {
                consumer.close()
            }
        }

    }
}