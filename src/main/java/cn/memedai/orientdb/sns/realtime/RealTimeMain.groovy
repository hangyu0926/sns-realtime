package cn.memedai.orientdb.sns.realtime

import org.springframework.context.ApplicationContext
import org.springframework.context.support.ClassPathXmlApplicationContext

/**
 * Created by kisho on 2017/6/8.
 */
class RealTimeMain {

    static void main(args) {
        /**
         * 反欺诈业务实时数据同步脚本入口
         */
        ApplicationContext context = null
        try {
            context = new ClassPathXmlApplicationContext('applicationContext.xml')
            context.getBean(RealTimeDispatch.class).start()
        } finally {
            if (context != null) {
                context.close()
            }
        }
    }

}