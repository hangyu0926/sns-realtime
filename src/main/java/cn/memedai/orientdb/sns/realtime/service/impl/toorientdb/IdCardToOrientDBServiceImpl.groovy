package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

import javax.annotation.Resource

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class IdCardToOrientDBServiceImpl implements RealTimeService {

    private static final LOG = LoggerFactory.getLogger(IdCardToOrientDBServiceImpl.class)

    @Resource
    private OrientSql orientDb

    void process(List<Map<String, Object>> dataList) {
        println dataList
        //TODO
    }

}
