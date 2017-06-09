package cn.memedai.orientdb.sns.realtime

import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import cn.memedai.orientdb.sns.realtime.service.impl.tomysql.ApplyToMysqlServiceImpl
import cn.memedai.orientdb.sns.realtime.service.impl.toorientdb.ApplyToOrientDBServiceImpl
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Service

import javax.annotation.Resource

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class RealTimeDispatch {

    private static final LOG = LoggerFactory.getLogger(RealTimeDispatch.class)

    //key为topic, value为处理topic的service
    //TODO
    private Map<String, List<RealTimeService>> topic2ServiceMap = [
            'Apply': [ApplyToOrientDBServiceImpl.class, ApplyToMysqlServiceImpl.class]
    ]

    @Resource
    private ApplicationContext context

    void dispatch(ConsumerRecords records) {
//        topic2ServiceMap.each {
//            key, value ->
//                Iterable<ConsumerRecord> crIt = records.records(key)
//                if (crIt != null && crIt.size() > 0) {
//                    List<String> dataList = new ArrayList<>(crIt.size())
//                    crIt.each {
//                        dataList.add(it)
//                    }
//                    value.each {
//                        context.getBean(it).process(dataList)
//                    }
//                }
//        }

        //TODO test
        def dataList = [
                [
                        'cellphone'       : '15821180279',
                        'member_id'       : 715157,
                        'apply_no'        : '1485313547297000',
                        'created_datetime': '2017-01-25 11:05:47',
                        'apply_status'    : 3000,
                        'store_id'        : '2759',
                        'order_no'        : '1496921804405003'
                ]
        ]
        context.getBean(ApplyToOrientDBServiceImpl.class).process(dataList)
    }

}
