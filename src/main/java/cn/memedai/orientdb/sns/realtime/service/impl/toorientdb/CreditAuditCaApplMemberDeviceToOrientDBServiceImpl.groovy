package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

import cn.memedai.orientdb.sns.realtime.cache.*
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service

import javax.annotation.Resource

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class CreditAuditCaApplMemberDeviceToOrientDBServiceImpl implements RealTimeService {

    private static final LOG = LoggerFactory.getLogger(CreditAuditCaApplMemberDeviceToOrientDBServiceImpl.class)

    @Resource
    private OrientSql orientSql

    @Resource
    private Sql sql

    @Resource
    private ApplyCache applyCache

    @Resource
    private DeviceCache deviceCache

    @Resource
    private IpCache ipCache

    @Resource
    private ApplyRidMemberRidCache applyRidMemberRidCache

    @Resource
    private MemberCache memberCache

    @Value("#{sqlProp.selectMemberFromApplyMysql}")
    private selectMemberFromApplyMysql

    void process(List<Map<String, Object>> dataList) {
        if (dataList == null) {
            return
        }

        if (dataList.size() == 0) {
            return
        }
        Map<String, Object> caIpAndDeviceMap = dataList.get(0)

        String appNo = caIpAndDeviceMap.APPL_NO
        if (StringUtils.isBlank(appNo)) {
            return
        }

        String op = caIpAndDeviceMap.__op__
        if ("update".equals(op)) {
            return
        }

        String applyRid = null
        CacheEntry applyCacheEntry = applyCache.get(appNo)
        if (applyCacheEntry != null) {
            applyRid = applyCacheEntry.value
        }

        if (StringUtils.isBlank(applyRid)) {
            return
        }

        //对DEVICE_ID加为空判断
        String deviceRid = null
        if (StringUtils.isNotBlank(caIpAndDeviceMap.DEVICE_ID)) {
            CacheEntry deviceCacheEntry = deviceCache.get(caIpAndDeviceMap.DEVICE_ID)
            if (deviceCacheEntry != null) {
                deviceRid = deviceCacheEntry.value
            }

            if (StringUtils.isNotBlank(deviceRid)) {
                orientSql.createEdge('ApplyHasDevice', applyRid, deviceRid)
            }
        }

        //对IP加为空判断
        String ipRid = null
        if (StringUtils.isNotBlank(caIpAndDeviceMap.IP)) {
            CacheEntry ipCacheEntry = ipCache.get(caIpAndDeviceMap.IP + "|" + caIpAndDeviceMap.IP_CITY)
            if (ipCacheEntry != null) {
                ipRid = ipCacheEntry.value
            }

            if (StringUtils.isNotBlank(ipRid)) {
                orientSql.createEdge('ApplyHasIp', applyRid, ipRid)
            }
        }


        CacheEntry applyRidMemberRidCacheEntry = applyRidMemberRidCache.get(applyRid)

        String memberRid = null
        if (null != applyRidMemberRidCacheEntry) {
            memberRid = applyRidMemberRidCacheEntry.value
        }

        //查mysql 根据apply查memberId
        if (null == memberRid) {
            int memberId = 0
            GroovyRowResult result =  sql.firstRow(selectMemberFromApplyMysql, [appNo])
            if (null == result){
                return
            }else{
                memberId = result.memberId
            }

            CacheEntry memberCacheEntry = memberCache.get(memberId)
            if (memberCacheEntry != null) {
                memberRid = memberCacheEntry.value
                applyRidMemberRidCache.put(new CacheEntry(applyRid, memberRid))
            }
        }

        if (StringUtils.isNotBlank(memberRid)) {
            if (StringUtils.isNotBlank(deviceRid)) {
                orientSql.createEdge('MemberHasDevice', memberRid, deviceRid)
            }
            if (StringUtils.isNotBlank(ipRid)) {
                orientSql.createEdge('MemberHasIp', memberRid, ipRid)
            }
        }

    }

}
