package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

import cn.memedai.orientdb.sns.realtime.cache.MemberCache
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service

import javax.annotation.Resource

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class AccountingFssLoansToOrientDBServiceImpl implements RealTimeService {

    private static final LOG = LoggerFactory.getLogger(AccountingFssLoansToOrientDBServiceImpl.class)

    @Resource
    private OrientSql orientSql

    @Resource
    private MemberCache memberCache

    @Value("#{snsOrientSqlProp.updateOverDueSql}")
    private String updateOverDueSql

    void process(List<Map<String, Object>> dataList) {
        if (dataList == null) {
            return
        }

        int size = dataList.size()
        for (def i = 0; i < size; i++) {
            Map<String, Object> loanMap = dataList.get(i)
            long memberId = loanMap.BORROWER_ID
            memberCache.get(memberId)

            if (null != loanMap.OVERDUE_DAYS && loanMap.OVERDUE_DAYS > 3) {
                orientSql.execute(updateOverDueSql, memberId)
            }
        }

    }

}
