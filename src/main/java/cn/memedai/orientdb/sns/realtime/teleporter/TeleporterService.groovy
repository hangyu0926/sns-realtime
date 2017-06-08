package cn.memedai.orientdb.sns.realtime.teleporter

import cn.memedai.orientdb.sns.realtime.common.OrientDb
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

import javax.annotation.Resource

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class TeleporterService {

    private static final LOG = LoggerFactory.getLogger(TeleporterService.class)

    @Resource
    private OrientDb orientDb

    void process(ConsumerRecord record) {
        LOG.info(orientDb.getDb().toString())
        //TODO
    }

}
