package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

import cn.memedai.orientdb.sns.realtime.cache.ApplyCache
import cn.memedai.orientdb.sns.realtime.cache.ApplyRidMemberRidCache
import cn.memedai.orientdb.sns.realtime.cache.CacheEntry
import cn.memedai.orientdb.sns.realtime.cache.DeviceCache
import cn.memedai.orientdb.sns.realtime.cache.IpCache
import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import cn.memedai.orientdb.sns.realtime.util.OrientSqlUtil
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

import javax.annotation.Resource

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class CaIpAndDeviceToOrientDBServiceImpl implements RealTimeService {

    private static final LOG = LoggerFactory.getLogger(CaIpAndDeviceToOrientDBServiceImpl.class)

    @Resource
    private OrientSql orientSql

    @Resource
    private ApplyCache applyCache

    @Resource
    private DeviceCache deviceCache

    @Resource
    private IpCache ipCache

    @Resource
    private ApplyRidMemberRidCache applyRidMemberRidCache

    void process(List<Map<String, Object>> dataList) {
        Map<String, Object> caIpAndDeviceMap = dataList.get(0)

        String appNo = caIpAndDeviceMap.APPL_NO
        if (null == appNo){
            return
        }

        String applyRid = applyCache.get(appNo).value
        if (null == applyRid){
            return
        }

        String deviceRid = deviceCache.get(caIpAndDeviceMap.DEVICE_ID).value
        orientSql.createEdge('ApplyHasDevice', applyRid, deviceRid)

        String ipRid = ipCache.get(caIpAndDeviceMap.IP+"|"+caIpAndDeviceMap.IP_CITY).value
        orientSql.createEdge('ApplyHasIp', applyRid, ipRid)


        String memberRid = applyRidMemberRidCache.get(applyRid).value
        orientSql.createEdge('MemberHasDevice', memberRid, deviceRid)
        orientSql.createEdge('MemberHasIp', memberRid, ipRid)
    }

}
