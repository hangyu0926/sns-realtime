package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

import cn.memedai.orientdb.sns.realtime.cache.*
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import cn.memedai.orientdb.sns.realtime.util.OrientSqlUtil
import com.orientechnologies.orient.core.sql.query.OResultSet
import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service

import javax.annotation.Resource
import java.text.MessageFormat

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class CreditAudit2CaBurOperatorContactToOrientDBServiceImpl implements RealTimeService {

    private static final LOG = LoggerFactory.getLogger(CreditAudit2CaBurOperatorContactToOrientDBServiceImpl.class)

    @Resource
    private ApplyCache applyCache

    @Resource
    private PhoneCache phoneCache

    @Resource
    private ApplyRidPhoneRidCache applyRidPhoneRidCache

    @Resource
    private ApplyHasDoCache applyHasDoneCache

    @Resource
    private OrientSql orientSql

    @Resource
    private Sql sql

    @Value("#{snsOrientSqlProp.checkCallToSql}")
    private String checkEdgeSql

    @Value("#{snsOrientSqlProp.createCallToSql}")
    private String createEdgeSql

    @Value("#{snsOrientSqlProp.updateCallToSql}")
    private String updateEdgeSql

    @Value("#{sqlProp.selectCallToInfoMySql}")
    private String selectCallToInfoMySql

    @Value("#{sqlProp.selectDirectMemberCountMySql}")
    private selectDirectMemberCountMySql

    @Value("#{sqlProp.selectPhoneFromApplyMySql}")
    private selectPhoneFromApplyMySql

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

        if ((applyHasDoneCache.get(appNo) != null) && (applyHasDoneCache.get(appNo).value != null)) {
            return
        }

        String op =  callToMap.__op__
        if ("update".equals(op)){
            return
        }

        int num = sql.firstRow(selectDirectMemberCountMySql,[appNo] as Object[]).num
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
        //查mysql 根据apply查phone
        if (null == fromPhoneRid){
            String phone = null
            GroovyRowResult result =  sql.firstRow(selectPhoneFromApplyMySql,[appNo])
            if (null == result){
                return
            }else{
                phone = result.phone
            }

            CacheEntry phoneCacheEntry = phoneCache.get(phone)
            if (phoneCacheEntry != null) {
                fromPhoneRid = phoneCacheEntry.value
                applyRidPhoneRidCache.put(new CacheEntry(appRid,fromPhoneRid))
            }
        }

        String toPhoneRid = null

        sql.rows(selectCallToInfoMySql, appNo).each {
            row ->
                String toPhone = row.PHONE_NO
                if (null == toPhone){
                    return
                }
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
                    orientSql.execute(MessageFormat.format(updateEdgeSql, OrientSqlUtil.getRid(ocrs)), args)
                }
        }
    }

}
