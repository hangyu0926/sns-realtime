package cn.memedai.orientdb.sns.realtime.service.impl.tomysql

import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import groovy.sql.Sql
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

import javax.annotation.Resource

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class CashLoanApplyToMysqlServiceImpl implements RealTimeService {

    private static final LOG = LoggerFactory.getLogger(CashLoanApplyToMysqlServiceImpl.class)

    @Resource
    private OrientSql orientSql

    @Resource
    private Sql sql

    void process(List<Map<String, Object>> dataList) {
        //TODO
    }

}
