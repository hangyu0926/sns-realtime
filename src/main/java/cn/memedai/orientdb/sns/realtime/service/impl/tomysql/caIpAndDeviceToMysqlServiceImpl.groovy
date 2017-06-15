package cn.memedai.orientdb.sns.realtime.service.impl.tomysql

import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import groovy.sql.Sql
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

import javax.annotation.Resource

/**
 * Created by hangyu on 2017/6/15.
 */
@Service
class caIpAndDeviceToMysqlServiceImpl {
    private static final LOG = LoggerFactory.getLogger(caIpAndDeviceToMysqlServiceImpl.class)

    @Resource
    private OrientSql orientSql

    @Resource
    private Sql sql

    void process(List<Map<String, Object>> dataList) {
        //TODO
    }
}
