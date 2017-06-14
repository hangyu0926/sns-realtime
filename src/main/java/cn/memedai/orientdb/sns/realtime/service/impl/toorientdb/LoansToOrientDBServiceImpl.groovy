package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

import cn.memedai.orientdb.sns.realtime.cache.MemberCache
import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

import javax.annotation.Resource

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class LoansToOrientDBServiceImpl implements RealTimeService {

    private static final LOG = LoggerFactory.getLogger(LoansToOrientDBServiceImpl.class)

    @Resource
    private OrientSql orientSql

    @Resource
    private MemberCache memberCache

    private String updateOverDueSql = 'update Member set isOverdue=true where memberId=?'

    void process(List<Map<String, Object>> dataList) {
        for (def i = 0; i < dataList.size(); i++){
            Map<String, Object> loanMap = dataList.get(i)

            if (null != loanMap.OVERDUE_DAYS && loanMap.OVERDUE_DAYS > 3){
                int memberId = loanMap.BORROWER_ID
                orientSql.execute(updateOverDueSql,memberId)
            }
        }

    }

}
