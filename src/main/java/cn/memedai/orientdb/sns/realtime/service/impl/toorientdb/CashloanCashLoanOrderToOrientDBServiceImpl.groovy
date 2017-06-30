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
class CashloanCashLoanOrderToOrientDBServiceImpl implements RealTimeService {

    private static final LOG = LoggerFactory.getLogger(CashloanCashLoanOrderToOrientDBServiceImpl.class)

    @Resource
    private MemberCache memberCache

    @Resource
    private PhoneCache phoneCache

    @Resource
    private ApplyCache applyCache

    @Resource
    private OrderCache orderCache

    @Resource
    private StoreCache storeCache

    @Resource
    private OrientSql orientSql

    @Value("#{snsOrientSqlProp.updateOrderSql}")
    private String updateOrderSql

    void process(List<Map<String, Object>> dataList) {
        if (dataList == null) {
            return
        }

        if (dataList.size() == 0) {
            return
        }
        Map<String, Object> orderMap = dataList.get(0)

        String op =  orderMap.__op__
        if ("update".equals(op)){
            return
        }

        String memberRid = null
        CacheEntry memberCacheEntry = memberCache.get(orderMap.member_id)
        if (memberCacheEntry != null) {
            memberRid = memberCacheEntry.value
        }

        String phoneRid = null
        CacheEntry phoneCacheEntry = phoneCache.get(orderMap.phoneNo)
        if (phoneCacheEntry != null) {
            phoneRid = phoneCacheEntry.value
        }

        String orderRid = OrientSqlUtil.getRid(orientSql.execute(updateOrderSql, orderMap.order_no, getStatus(orderMap.status), orderMap.status,
                orderMap.amount, orderMap.created_datetime, orderMap.order_no))
        if (StringUtils.isNotBlank(orderRid)) {
            orderCache.put(new CacheEntry(orderMap.order_no, orderRid))
        }

        if (StringUtils.isNotBlank(memberRid) && StringUtils.isNotBlank(phoneRid)) {
            orientSql.createEdge('HasPhone', memberRid, phoneRid)
        }

        if (StringUtils.isNotBlank(memberRid) && StringUtils.isNotBlank(orderRid)) {
            orientSql.createEdge('MemberHasOrder', memberRid, orderRid)
        }

        if (StringUtils.isNotBlank(phoneRid) && StringUtils.isNotBlank(orderRid)) {
            orientSql.createEdge('PhoneHasOrder', phoneRid, orderRid)
        }

        if (orderMap.source != null) {
            String storeRid = null
            String storeId = getStoreId(orderMap.source)
            CacheEntry storeCacheEntry = storeCache.get(storeId)
            if (storeCacheEntry != null) {
                storeRid = storeCacheEntry.value
            }

            if (StringUtils.isNotBlank(storeRid) && StringUtils.isNotBlank(orderRid)) {
                orientSql.createEdge('OrderHasStore', orderRid, storeRid)
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
            return '3515'
        }
    }

    private Integer getStatus(def status) {
        if (status == null) {
            return 0
        }
        if (status.toString() == '40' || status.toString() == '70' || status.toString() == '80') {
            return 1
        } else if (status.toString() == '10') {
            return null
        } else {
            return 0
        }
    }

}
