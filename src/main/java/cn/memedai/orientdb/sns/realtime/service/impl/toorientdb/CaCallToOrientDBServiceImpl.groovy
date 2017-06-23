package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

import cn.memedai.orientdb.sns.realtime.cache.*
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import cn.memedai.orientdb.sns.realtime.sql.OrientSql
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

    private String selectCallToInfoSql = 'select APPL_NO,PHONE_NO,CALL_CNT,CALL_LEN,CALL_IN_CNT,CALL_OUT_CNT,CREATE_TIME from network.ca_bur_operator_contact where PHONE_NO is not null and APPL_NO =?'

    private selectDirectMemberCountSql = 'SELECT COUNT(*) AS num FROM member_index where apply_no = ? and direct = "contact_accept_member_num"'

    void process(List<Map<String, Object>> dataList) {
        if (dataList == null) {
            return
        }

        if (dataList.size() == 0) {
            return
        }
        Map<String, Object> callToMap = dataList.get(0)

        String appNo = (String) callToMap.APPL_NO

        if (StringUtils.isBlank(appNo)) {
            return
        }

        if ((applyHasDoCache.get(appNo) != null) && (applyHasDoCache.get(appNo).value != null)) {
            return
        }

        String op =  callToMap.__op__
        if ("update".equals(op)){
            return
        }

        int num = sql.firstRow(selectDirectMemberCountSql,[appNo] as Object[]).num
        if (num > 0){
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

        String toPhoneRid = null

        sql.query(selectCallToInfoSql, appNo) {
            row ->
                String toPhone = row.PHONE_NO
                int callCnt = row.CALL_CNT
                int callLen = row.CALL_LEN
                int callInCnt = row.CALL_IN_CNT
                int callOutCnt = row.CALL_OUT_CNT
                String createTime = row.CREATE_TIME

                CacheEntry phoneCacheEntry = phoneCache.get(toPhone)
                if (phoneCacheEntry != null) {
                    toPhoneRid = phoneCacheEntry.value
                }
                if (StringUtils.isBlank(toPhoneRid)) {
                    return
                }

                def args = [callCnt, callLen, callInCnt, callOutCnt, createTime] as Object[]

                OResultSet ocrs = orientSql.execute(MessageFormat.format(checkEdgeSql, "CallTo", fromPhoneRid, toPhoneRid))
                if (CollectionUtils.isEmpty(ocrs)) {
                    orientSql.execute(MessageFormat.format(createEdgeSql, fromPhoneRid, toPhoneRid), args)
                } else {
                    ODocument doc = (ODocument) ocrs.get(0)
                    ORecordId oRecordId = doc.field("@rid")
                    orientSql.execute(MessageFormat.format(updateEdgeSql, oRecordId.getIdentity().toString()), args)
                }
        }

        //写入缓存
        applyHasDoCache.put(new CacheEntry(appNo, true))
    }

}
