package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

import cn.memedai.orientdb.sns.realtime.cache.CacheEntry
import cn.memedai.orientdb.sns.realtime.cache.StoreCache
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import cn.memedai.orientdb.sns.realtime.util.OrientSqlUtil
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

import javax.annotation.Resource

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class StoreToOrientDBServiceImpl implements RealTimeService {

    private static final LOG = LoggerFactory.getLogger(StoreToOrientDBServiceImpl.class)

    @Resource
    private StoreCache storeCache

    @Resource
    private OrientSql orientSql

    private String updateStoreSql = 'update Store set storeId=?,merchantId=?,storeName=?,province=?,city=?,creditLimitType=?,policyBracket=?,businessFirstType=? upsert return after where storeId = ?'

    void process(List<Map<String, Object>> dataList) {
        for (def i = 0; i < dataList.size(); i++){
            Map<String, Object> storeMap = dataList.get(i)

            String storeRid = OrientSqlUtil.getRid(orientSql.execute(updateStoreSql, storeMap.STOREID,storeMap.MERCHANTID,storeMap.STORENAME,storeMap.PROVINCE,storeMap.CITY,storeMap.CREDIT_LIMIT_TYPE
                    ,storeMap.POLICY_BRACKET,storeMap.BUSINESS_FIRST_TYPE ))

            storeCache.put(new CacheEntry(storeMap.store_id, storeRid))
        }


    }

}
