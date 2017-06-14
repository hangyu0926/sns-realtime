package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

import cn.memedai.orientdb.sns.realtime.cache.ApplyCache
import cn.memedai.orientdb.sns.realtime.cache.ApplyHasDoCache
import cn.memedai.orientdb.sns.realtime.cache.ApplyRidPhoneRidCache
import cn.memedai.orientdb.sns.realtime.cache.CacheEntry
import cn.memedai.orientdb.sns.realtime.cache.PhoneCache

import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import com.orientechnologies.orient.core.id.ORecordId
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.query.OResultSet
import groovy.sql.Sql
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

import javax.annotation.Resource
import java.text.MessageFormat

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class CaCallToOrientDBServiceImpl implements RealTimeService {

    private static final LOG = LoggerFactory.getLogger(CaCallToOrientDBServiceImpl.class)

    @Resource
    private ApplyCache applyCache

    @Resource
    private PhoneCache phoneCache

    @Resource
    private ApplyRidPhoneRidCache applyRidPhoneRidCache

    @Resource
    private ApplyHasDoCache applyHasDoCache

    @Resource
    private OrientSql orientSql

    @Resource
    private Sql sql

    private String checkEdgeSql = 'select from (select expand(out_{0}) from {1}) where in = {2}'

    private String createEdgeSql = 'create edge CallTo from {0} to {1} set callCnt = ?,callLen=?,callInCnt=?,callOutCnt=?,reportTime=?'

    private String updateEdgeSql = 'update edge {0} set callCnt = ?,callLen=?,callInCnt=?,callOutCnt=?,reportTime=?'

    void process(List<Map<String, Object>> dataList) {
        if (dataList == null) {
            return
        }

        if (dataList.size() == 0) {
            return
        }
        Map<String, Object> callToMap = dataList.get(0)

        String appNo = (String) callToMap.APPL_NO

        LOG.info("appNo is {}",appNo)
        if (StringUtils.isBlank(appNo)) {
            return
        }

        if ((applyHasDoCache.get(appNo) != null) && (applyHasDoCache.get(appNo).value != null)) {
            return
        }

        String appRid = null
        CacheEntry applyCacheEntry = applyCache.get(appNo)
        if (applyCacheEntry != null) {
            appRid = applyCacheEntry.value
        }
        if (StringUtils.isBlank(appRid)) {
            return
        }

        String fromPhoneRid = null
        CacheEntry applyRidPhoneRidCacheEntry = applyRidPhoneRidCache.get(appRid)
        if (applyRidPhoneRidCacheEntry != null) {
            fromPhoneRid = applyRidPhoneRidCacheEntry.value
        }
        if (StringUtils.isBlank(fromPhoneRid)) {
            return
        }

        LOG.info("fromPhoneRid is {}",fromPhoneRid)

        String toPhoneRid = null

        sql.query('select APPL_NO,PHONE_NO,CALL_CNT,CALL_LEN,CALL_IN_CNT,CALL_OUT_CNT,CREATE_TIME from network.ca_bur_operator_contact where PHONE_NO is not null and APPL_NO = '+ appNo) {
            rs ->
                while (rs.next()) {
                    String toPhone = rs.getString("PHONE_NO")
                    int callCnt = rs.getInt("CALL_CNT")
                    int callLen = rs.getInt("CALL_LEN")
                    int callInCnt = rs.getInt("CALL_IN_CNT")
                    int callOutCnt = rs.getInt("CALL_OUT_CNT")
                    String createTime = rs.getString("CREATE_TIME")

                    CacheEntry phoneCacheEntry = phoneCache.get(toPhone)
                    if (phoneCacheEntry != null) {
                        toPhoneRid = phoneCacheEntry.value
                    }
                    if (StringUtils.isBlank(toPhoneRid)) {
                        continue
                    }

                    def args = [callCnt, callLen, callInCnt, callOutCnt, createTime] as Object[]

                    OResultSet ocrs = orientSql.execute(MessageFormat.format(checkEdgeSql, "CallTo", fromPhoneRid, toPhoneRid))
                    if (CollectionUtils.isEmpty(ocrs)) {
                        orientSql.execute(MessageFormat.format(createEdgeSql, fromPhoneRid, toPhoneRid), args)
                    } else {
                        ODocument doc = (ODocument) ocrs.get(0)
                        ORecordId oRecordId = doc.field("@rid")
                        orientSql.execute(MessageFormat.format(updateEdgeSql, oRecordId.getIdentity().toString()),args)
                    }
                }
        }

        //写入缓存
        applyHasDoCache.put(new CacheEntry(appNo, true))
    }

}
