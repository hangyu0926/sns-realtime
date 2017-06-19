package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class CashLoanApplyToOrientDBServiceImpl implements RealTimeService {

    private static final LOG = LoggerFactory.getLogger(CashLoanApplyToOrientDBServiceImpl.class)

    void process(List<Map<String, Object>> dataList) {
        //TODO
    }

}
