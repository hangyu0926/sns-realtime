package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

import cn.memedai.orientdb.sns.realtime.cache.*
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory
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
    private OrderCache orderCache

    @Resource
    private DeviceCache deviceCache

    @Resource
    private IpCache ipCache

    @Resource
    private OrderRidMemberRidCache orderRidMemberRidCache

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

        String orderRid = null
        CacheEntry orderCacheEntry = orderCache.get(orderNo)
        if (orderCacheEntry != null) {
            orderRid = orderCacheEntry.value
        }

        if (StringUtils.isBlank(orderRid)) {
            return
        }

        String deviceRid = null
        CacheEntry deviceCacheEntry = deviceCache.get(ctaIpAndDeviceMap.DEVICE_ID)
        if (deviceCacheEntry != null) {
            deviceRid = deviceCacheEntry.value
        }

        if (StringUtils.isNotBlank(deviceRid)) {
            orientSql.createEdge('OrderHasDevice', orderRid, deviceRid)
        }

        String ipRid = null
        CacheEntry ipCacheEntry = ipCache.get(ctaIpAndDeviceMap.IP + "|" + ctaIpAndDeviceMap.IP_CITY)
        if (ipCacheEntry != null) {
            ipRid = ipCacheEntry.value
        }

        if (StringUtils.isNotBlank(ipRid)) {
            orientSql.createEdge('OrderHasIp', orderRid, ipRid)
        }


        CacheEntry applyRidMemberRidCacheEntry = orderRidMemberRidCache.get(orderRid)

        String memberRid = null
        if (null != applyRidMemberRidCacheEntry) {
            memberRid = applyRidMemberRidCacheEntry.value
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
