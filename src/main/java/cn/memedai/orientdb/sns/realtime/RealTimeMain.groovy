package cn.memedai.orientdb.sns.realtime

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
        final Thread t = new Thread() {
            @Override
            void run() {
                ApplicationContext context = null
                try {
                    context = new ClassPathXmlApplicationContext('applicationContext.xml')
                    context.getBean(RealTimeDispatch.class).start()
                } catch (Exception e) {
                    LOG.error("", e)
                } finally {
                    if (context != null) {
                        context.close()
                    }
                }
            }
        }

        t.setDaemon(false)

        t.start()
        t.join()
    }

}