package cn.memedai.orientdb.sns.realtime.batch

import cn.memedai.orientdb.sns.realtime.common.OrientDb
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
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
    private JdbcTemplate jdbcTemplate

    void process(ConsumerRecord record) {
        LOG.debug(orientDb.getDb().toString())
        LOG.debug(jdbcTemplate.toString())
        //TODO
    }

}
