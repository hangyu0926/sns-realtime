package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

import cn.memedai.orientdb.sns.realtime.cache.*
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import cn.memedai.orientdb.sns.realtime.util.OrientSqlUtil
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

import javax.annotation.Resource

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class OrderToOrientDBServiceImpl implements RealTimeService {

    private static final LOG = LoggerFactory.getLogger(OrderToOrientDBServiceImpl.class)

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

    private String updateOrderSql = 'update Order set orderNo=?,status=?,originalStatus=?,amount=?,createdDatetime=? upsert return after where orderNo=?'

    void process(List<Map<String, Object>> dataList) {
        if (dataList == null) {
            return
        }

        if (dataList.size() == 0) {
            return
        }
        Map<String, Object> orderMap = dataList.get(0)

        String memberRid = null
        CacheEntry memberCacheEntry = memberCache.get(orderMap.member_id)
        if (memberCacheEntry != null) {
            memberRid = memberCacheEntry.value
        }

        String phoneRid = null
        CacheEntry phoneCacheEntry = phoneCache.get(orderMap.mobile)
        if (phoneCacheEntry != null) {
            phoneRid = phoneCacheEntry.value
        }

        String orderRid = OrientSqlUtil.getRid(orientSql.execute(updateOrderSql, orderMap.order_no, getStatus(orderMap.status), orderMap.status,
                (orderMap.pay_amount) / 100, orderMap.created_datetime, orderMap.order_no))

        String op =  orderMap.__op__
        if ("update".equals(op)){
            return
        }

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

        if (orderMap.store_id != null) {
            String storeRid = null
            CacheEntry storeCacheEntry = storeCache.get(orderMap.store_id)
            if (storeCacheEntry != null) {
                storeRid = storeCacheEntry.value
            }

            if (StringUtils.isNotBlank(orderRid) && StringUtils.isNotBlank(storeRid)) {
                orientSql.createEdge('OrderHasStore', orderRid, storeRid)
            }
        }

    }

    private Integer getStatus(def status) {
        if (status == null) {
            return 0
        }
        if (status.toString() == '1051' || status.toString() == '1081' || status.toString() == '1085') {
            return 1
        } else if (status.toString() == '1011' || status.toString() == '1012') {
            return null
        } else {
            return 0
        }
    }
}
