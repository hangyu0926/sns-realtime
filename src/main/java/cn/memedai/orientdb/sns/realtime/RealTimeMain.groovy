package cn.memedai.orientdb.sns.realtime

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
//            while (true) {
                context.getBean(RealTimeDispatch.class).dispatch(null)
//                Thread.sleep(4000)
//            }

//            consumer = new KafkaConsumer<>(context.getBean('kafkaProp'))
//            consumer.subscribe(context.getBean('kafkaTopics'))
//            while (true) {
//                ConsumerRecords records = null
//                try {
//                    records = consumer.poll(Long.MAX_VALUE)
//                    context.getBean(RealTimeDispatch.class).dispatch(records)
//
//                    consumer.commitAsync()
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
            if (consumer != null) {
                consumer.close()
            }
        }

    }

}