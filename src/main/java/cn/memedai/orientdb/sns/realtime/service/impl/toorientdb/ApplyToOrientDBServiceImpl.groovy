package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

import cn.memedai.orientdb.sns.realtime.cache.*
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import cn.memedai.orientdb.sns.realtime.util.OrientSqlUtil
import org.slf4j.LoggerFactory
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

    private String updateApplySql = 'update Apply set applyNo=?,status=?,originalStatus=?,createdDatetime=? upsert return after where applyNo=?'

    void process(List<Map<String, Object>> dataList) {
        Map<String, Object> applyMap = dataList.get(0)

        String memberRid = memberCache.get(applyMap.member_id).value

        String phoneRid = phoneCache.get(applyMap.cellphone).value

        String applyRid = OrientSqlUtil.getRid(orientSql.execute(updateApplySql, applyMap.apply_no, getStatus(applyMap.apply_status), applyMap.apply_status, applyMap.created_datetime, applyMap.apply_no))
        applyCache.put(new CacheEntry(applyMap.apply_no, applyRid))

        orientSql.createEdge('HasPhone', memberRid, phoneRid)
        orientSql.createEdge('MemberHasApply', memberRid, applyRid)
        orientSql.createEdge('PhoneHasApply', phoneRid, applyRid)

        if (applyMap.order_no != null) {
            String orderRid = orderCache.get(applyMap.order_no).value
            orientSql.createEdge('MemberHasOrder', memberRid, orderRid)
            orientSql.createEdge('PhoneHasOrder', phoneRid, orderRid)
            orientSql.createEdge('ApplyHasOrder', applyRid, orderRid)
        }

        if (applyMap.store_id != null) {
            String storeRid = storeCache.get(applyMap.store_id).value
            orientSql.createEdge('ApplyHasStore', applyRid, storeRid)
        }
        LOG.info("processed apply => $applyMap.apply_no")
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
