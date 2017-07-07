package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

import cn.memedai.orientdb.sns.realtime.cache.*
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import cn.memedai.orientdb.sns.realtime.util.OrientSqlUtil
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service

import javax.annotation.Resource

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class CashloanApplyInfoToOrientDBServiceImpl implements RealTimeService {

    private static final LOG = LoggerFactory.getLogger(CashloanApplyInfoToOrientDBServiceImpl.class)

    @Resource
    private OrientSql orientSql

    @Resource
    private MemberCache memberCache

    @Resource
    private PhoneCache phoneCache

    @Resource
    private ApplyCache applyCache

    @Resource
    private OrderCache orderCache

    @Resource
    private ApplyNoOrderNoCache applyNoOrderNoCache

    @Resource
    private StoreCache storeCache

    @Resource
    private DeviceCache deviceCache

    @Resource
    private IpCache ipCache

    @Value("#{snsOrientSqlProp.updateCashLoanApplySql}")
    private String updateCashLoanApplySql

    void process(List<Map<String, Object>> dataList) {
        if (dataList == null) {
            return
        }

        if (dataList.size() == 0) {
            return
        }
        Map<String, Object> applyMap = dataList.get(0)

        String op =  applyMap.__op__
        if ("update".equals(op)){
            return
        }

        String memberRid = null
        CacheEntry memberCacheEntry = memberCache.get(applyMap.member_id)
        if (memberCacheEntry != null) {
            memberRid = memberCacheEntry.value
        }

        String phoneRid = null
        CacheEntry phoneCacheEntry = phoneCache.get(applyMap.cellphone)
        if (phoneCacheEntry != null) {
            phoneRid = phoneCacheEntry.value
        }

        String applyNo = applyMap.apply_no

        String applyRid = OrientSqlUtil.getRid(orientSql.execute(updateCashLoanApplySql, applyNo, applyMap.created_datetime, applyMap.apply_no))
        if (StringUtils.isNotBlank(applyRid)) {
            applyCache.put(new CacheEntry(applyMap.apply_no, applyRid))
        }

        if (StringUtils.isNotBlank(memberRid) && StringUtils.isNotBlank(phoneRid)) {
            orientSql.createEdge('HasPhone', memberRid, phoneRid)
        }

        if (StringUtils.isNotBlank(memberRid) && StringUtils.isNotBlank(applyRid)) {
            orientSql.createEdge('MemberHasApply', memberRid, applyRid)
        }

        if (StringUtils.isNotBlank(phoneRid) && StringUtils.isNotBlank(applyRid)) {
            orientSql.createEdge('PhoneHasApply', phoneRid, applyRid)
        }

        String orderNo = applyNo
        //将apply和order关系放入缓存
        applyNoOrderNoCache.put(new CacheEntry(applyNo,orderNo))

        String orderRid = null
        CacheEntry orderCacheEntry = orderCache.get(orderNo)
        if (orderCacheEntry != null) {
            orderRid = orderCacheEntry.value
        }

        if (StringUtils.isNotBlank(orderRid)) {
            if (StringUtils.isNotBlank(memberRid)) {
                orientSql.createEdge('MemberHasOrder', memberRid, orderRid)
            }

            if (StringUtils.isNotBlank(phoneRid)) {
                orientSql.createEdge('PhoneHasOrder', phoneRid, orderRid)
            }

            if (StringUtils.isNotBlank(applyRid)) {
                orientSql.createEdge('ApplyHasOrder', applyRid, orderRid)
            }
        }

        if (StringUtils.isNotBlank(applyMap.source)) {
            String storeRid = null
            String storeId = getStoreId(applyMap.source)
            CacheEntry storeCacheEntry = storeCache.get(storeId)
            if (storeCacheEntry != null) {
                storeRid = storeCacheEntry.value
            }

            if (StringUtils.isNotBlank(storeRid) && StringUtils.isNotBlank(applyRid)) {
                orientSql.createEdge('ApplyHasStore', applyRid, storeRid)
            }
        }

        String deviceRid = null
        if (StringUtils.isNotBlank(applyMap.device_id)) {
            CacheEntry deviceCacheEntry = deviceCache.get(applyMap.device_id)
            if (deviceCacheEntry != null) {
                deviceRid = deviceCacheEntry.value
            }

            if (StringUtils.isNotBlank(deviceRid)) {
                orientSql.createEdge('ApplyHasDevice', applyRid, deviceRid)
            }
        }

        String ipRid = null
        if (StringUtils.isNotBlank(applyMap.ip1)) {
            CacheEntry ipCacheEntry = ipCache.get(applyMap.ip1 + "|" + applyMap.ip1_city)
            if (ipCacheEntry != null) {
                ipRid = ipCacheEntry.value
            }

            if (StringUtils.isNotBlank(ipRid)) {
                orientSql.createEdge('ApplyHasIp', applyRid, ipRid)
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

    private String getStoreId(def source) {
        if (source == null) {
            return null
        }
        if (source.toString() == '4') {
            return '3709'
        } else if (source.toString() == '0') {
            return '3515'
        } else {
            return null
        }
    }
}
