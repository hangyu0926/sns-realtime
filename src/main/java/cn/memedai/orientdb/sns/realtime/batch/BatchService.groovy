package cn.memedai.orientdb.sns.realtime.batch

import cn.memedai.orientdb.sns.realtime.common.OrientDb
import groovy.sql.Sql
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

import javax.annotation.Resource

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class BatchService {

    private static final LOG = LoggerFactory.getLogger(BatchService.class)

    @Resource
    private OrientDb orientDb

    @Resource
    private Sql sql

    void process(ConsumerRecords records) {
        LOG.debug(orientDb.getDb().toString())
        sql.eachRow('select count(1) as num from network.apply_info', {
            row ->
                LOG.info("" + row.num)
        })
        //TODO
    }

}
