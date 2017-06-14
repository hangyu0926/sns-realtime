package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

import cn.memedai.orientdb.sns.realtime.cache.DeviceCache
import cn.memedai.orientdb.sns.realtime.cache.IpCache
import cn.memedai.orientdb.sns.realtime.cache.OrderCache
import cn.memedai.orientdb.sns.realtime.cache.OrderRidMemberRidCache
import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
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
        Map<String, Object> ctaIpAndDeviceMap = dataList.get(0)

        String orderNo = ctaIpAndDeviceMap.ORDER_ID
        if (null == orderNo){
            return
        }

        String orderRid = orderCache.get(orderNo).value
        if (null == orderRid){
            return
        }

        String deviceRid = deviceCache.get(ctaIpAndDeviceMap.DEVICE_ID).value
        orientSql.createEdge('OrderHasDevice', orderRid, deviceRid)

        String ipRid = ipCache.get(ctaIpAndDeviceMap.IP+"|"+ctaIpAndDeviceMap.IP_CITY).value
        orientSql.createEdge('OrderHasIp', orderRid, ipRid)


        String memberRid = orderRidMemberRidCache.get(orderRid).value
        orientSql.createEdge('MemberHasDevice', memberRid, deviceRid)
        orientSql.createEdge('MemberHasIp', memberRid, ipRid)
    }

}
