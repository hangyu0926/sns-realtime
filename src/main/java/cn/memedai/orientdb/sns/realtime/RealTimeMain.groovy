package cn.memedai.orientdb.sns.realtime

import org.springframework.context.support.ClassPathXmlApplicationContext

/**
 * Created by kisho on 2017/6/8.
 */
class RealTimeMain {

    static void main(args) {
        /**
         * 反欺诈业务实时数据同步脚本入口
         */
        new ClassPathXmlApplicationContext('applicationContext.xml')
                .getBean(RealTimeDispatch.class)
                .start()
    }

}