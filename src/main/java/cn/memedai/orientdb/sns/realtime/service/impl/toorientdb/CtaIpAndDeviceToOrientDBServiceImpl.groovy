package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

import cn.memedai.orientdb.sns.realtime.cache.*
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import cn.memedai.orientdb.sns.realtime.sql.OrientSql
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
class CtaIpAndDeviceToOrientDBServiceImpl implements RealTimeService {

    private static final LOG = LoggerFactory.getLogger(CtaIpAndDeviceToOrientDBServiceImpl.class)

    @Resource
    private OrientSql orientSql

    @Resource
    private Sql sql

    @Resource
    private OrderCache orderCache

    @Resource
    private DeviceCache deviceCache

    @Resource
    private IpCache ipCache

    @Resource
    private OrderRidMemberRidCache orderRidMemberRidCache

    @Resource
    private MemberCache memberCache

    @Value("#{sqlProp.selectMemberFromOrderMysql}")
    private selectMemberFromOrderMysql

    void process(List<Map<String, Object>> dataList) {
        if (dataList == null) {
            return
        }

        if (dataList.size() == 0) {
            return
        }
        Map<String, Object> ctaIpAndDeviceMap = dataList.get(0)

        String orderNo = ctaIpAndDeviceMap.ORDER_ID
        if (StringUtils.isBlank(orderNo)) {
            return
        }

        String op =  ctaIpAndDeviceMap.__op__
        if ("update".equals(op)){
            return
        }

        String orderRid = null
        CacheEntry orderCacheEntry = orderCache.get(orderNo)
        if (orderCacheEntry != null) {
            orderRid = orderCacheEntry.value
        }

        if (StringUtils.isBlank(orderRid)) {
            return
        }

        //对DEVICE_ID加为空判断
        String deviceRid = null
        if (StringUtils.isNotBlank(ctaIpAndDeviceMap.DEVICE_ID)){
            CacheEntry deviceCacheEntry = deviceCache.get(ctaIpAndDeviceMap.DEVICE_ID)
            if (deviceCacheEntry != null) {
                deviceRid = deviceCacheEntry.value
            }

            if (StringUtils.isNotBlank(deviceRid)) {
                orientSql.createEdge('OrderHasDevice', orderRid, deviceRid)
            }
        }


        //对IP加为空判断
        String ipRid = null
        if (StringUtils.isNotBlank(ctaIpAndDeviceMap.IP)){
            CacheEntry ipCacheEntry = ipCache.get(ctaIpAndDeviceMap.IP + "|" + ctaIpAndDeviceMap.IP_CITY)
            if (ipCacheEntry != null) {
                ipRid = ipCacheEntry.value
            }

            if (StringUtils.isNotBlank(ipRid)) {
                orientSql.createEdge('OrderHasIp', orderRid, ipRid)
            }

        }


        CacheEntry applyRidMemberRidCacheEntry = orderRidMemberRidCache.get(orderRid)

        String memberRid = null
        if (null != applyRidMemberRidCacheEntry) {
            memberRid = applyRidMemberRidCacheEntry.value
        }

        //查mysql 根据apply查memberId
        if (null == memberRid){
            int memberId = sql.firstRow(selectMemberFromOrderMysql,[orderNo]).memberId
            CacheEntry memberCacheEntry = memberCache.get(memberId)
            if (memberCacheEntry != null) {
                memberRid = memberCacheEntry.value
                orderRidMemberRidCache.put(new CacheEntry(orderRid,memberRid))
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
