package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

/**
 * Created by hangyu on 2017/6/19.
 */
@Service
class CreditAuditApplyToOrientDBServiceImpl implements RealTimeService{
    private static final LOG = LoggerFactory.getLogger(CreditAuditApplyToOrientDBServiceImpl.class)

    void process(List<Map<String, Object>> dataList) {
        //TODO
    }
}
