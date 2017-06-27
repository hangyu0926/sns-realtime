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
class ApplyToOrientDBServiceImpl implements RealTimeService {

    private static final LOG = LoggerFactory.getLogger(ApplyToOrientDBServiceImpl.class)

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
    private StoreCache storeCache

    //在CaCallToMysql中用到
    @Resource
    private ApplyNoOrderNoCache applyNoOrderNoCache

    //在CaCallToMysql中用到
    @Resource
    private ApplyNoPhoneCache applyNoPhoneCache

    //在CaIpOrientDb中用到
    @Resource
    private ApplyRidMemberRidCache applyRidMemberRidCache

    //在CaCallToOrientDb中用到
    @Resource
    private ApplyRidPhoneRidCache applyRidPhoneRidCache

    @Value("#{snsOrientSqlProp.updateApplySql}")
    private String updateApplySql

    void process(List<Map<String, Object>> dataList) {
        if (dataList == null) {
            return
        }

        if (dataList.size() == 0) {
            return
        }
        Map<String, Object> applyMap = dataList.get(0)

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

        String applyRid = OrientSqlUtil.getRid(orientSql.execute(updateApplySql, applyMap.apply_no, getStatus(applyMap.apply_status), applyMap.apply_status, applyMap.created_datetime, applyMap.apply_no))
        if (StringUtils.isNotBlank(applyRid)) {
            applyCache.put(new CacheEntry(applyMap.apply_no, applyRid))
            applyRidMemberRidCache.put(new CacheEntry(applyRid, memberRid))
        }

        //将apply和phone关系放入缓存
        applyNoPhoneCache.put(new CacheEntry(applyMap.apply_no,applyMap.cellphone))

        String op =  applyMap.__op__
        if ("update".equals(op)){
            return
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

        if (applyMap.order_no != null) {
            //将apply和order关系放入缓存
            applyNoOrderNoCache.put(new CacheEntry(applyMap.apply_no,applyMap.order_no))

            String orderRid = null
            CacheEntry orderCacheEntry = orderCache.get(applyMap.order_no)
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
        }

        if (applyMap.store_id != null) {
            String storeRid = null
            CacheEntry storeCacheEntry = storeCache.get(applyMap.store_id)
            if (storeCacheEntry != null) {
                storeRid = storeCacheEntry.value
            }

            if (StringUtils.isNotBlank(storeRid) && StringUtils.isNotBlank(applyRid)) {
                orientSql.createEdge('ApplyHasStore', applyRid, storeRid)
            }
        }
    }

    private Integer getStatus(def status) {
        if (status == null) {
            return null
        }
        if (status.toString() == '4000') {
            return 1
        } else if (status.toString().startsWith('30')) {
            return 0
        } else {
            return null
        }
    }
}
