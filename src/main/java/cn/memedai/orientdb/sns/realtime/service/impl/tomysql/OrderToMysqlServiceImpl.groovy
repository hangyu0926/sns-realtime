package cn.memedai.orientdb.sns.realtime.service.impl.tomysql

import cn.memedai.orientdb.sns.realtime.bean.IndexData
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import com.orientechnologies.orient.core.db.record.OIdentifiable
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.query.OBasicResultSet
import groovy.sql.Sql
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

import javax.annotation.Resource

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class OrderToMysqlServiceImpl implements RealTimeService {

    private static final LOG = LoggerFactory.getLogger(OrderToMysqlServiceImpl.class)

    @Resource
    private OrientSql orientSql

    @Resource
    private Sql sql

    private selectOrderFromApplySql = 'select in("ApplyHasOrder").applyNo as applyNo,in("ApplyHasOrder").originalStatus as applyStatus from order where orderNo = ? unwind applyNo,applyStatus'

    private selectMemberSql = 'select out("MemberHasDevice").size() as MemberHasDeviceSize,out("MemberHasIp").size() as MemberHasIpSize,' +
            'out("MemberHasApply").size() as MemberHasApplySize,out("MemberHasOrder").size() as MemberHasOrderSize,@rid as members0 from member where memberId = ?'

    private selectMemberCountFromOrderSql = 'SELECT COUNT(*) AS num FROM member_index where order_no = ? and direct = "has_device_num"'

    void process(List<Map<String, Object>> dataList) {
        if (dataList == null || dataList.size() == 0) {
            return
        }

        Map<String, Object> orderMap = dataList.get(0)

        String orderNo = (String) orderMap.order_no
        if (StringUtils.isBlank(orderNo)) {
            return
        }
        String phone = (String) orderMap.mobile
        if (StringUtils.isBlank(phone)) {
            return
        }
        long memberId = orderMap.member_id
        if (StringUtils.isBlank(String.valueOf(memberId))) {
            return
        }

        String orderStatus = (String) orderMap.status

        String appNo = null
        String applyStatus = null

        OBasicResultSet orderNoResult = orientSql.execute(selectOrderFromApplySql, orderNo)
        if (null != orderNoResult && orderNoResult.size() > 0) {
            ODocument orderNoDocument = orderNoResult.get(0)
            appNo = orderNoDocument.field("applyNo") != null ? orderNoDocument.field("applyNo").toString() : null
            applyStatus = orderNoDocument.field("applyStatus") != null ? orderNoDocument.field("applyStatus").toString() : null
        }

        String op =  orderMap.__op__

        if ("insert".equals(op)){
            //如果存在appNo说明apply已经先来了不需要做任何操作
            if (null != appNo) {
                return
            }
            //如果不存在说明Order先来或者压根没有apply都只要做统计插入即可
            if (appNo == null) {
                List<IndexData> memberIndexDatas = new ArrayList<IndexData>()
                structureMemberIndexDatas(memberId,phone,appNo,orderNo,applyStatus,orderStatus,memberIndexDatas)
                insertMemberIndex(memberIndexDatas)
            }
        }

        if ("update".equals(op)){
            List<IndexData> memberIndexDatas = new ArrayList<IndexData>()
            structureMemberIndexDatas(memberId,phone,appNo,orderNo,applyStatus,orderStatus,memberIndexDatas)
            //判断下数据库中是否有值
            int memberCount = 0
            memberCount = sql.firstRow(selectMemberCountFromOrderSql,[orderNo] as Object[]).num
            if (memberCount > 0){
                updateMemberIndex(memberIndexDatas)
            }else{
                insertMemberIndex(memberIndexDatas)
            }
        }
    }

    private void structureMemberIndexDatas(long memberId,String phone,String appNo,String orderNo,String applyStatus,String orderStatus, List<IndexData> memberIndexDatas){
        OBasicResultSet memberResult = orientSql.execute(selectMemberSql, memberId)
        if (null != memberResult && memberResult.size() > 0) {
            ODocument memberDocument = memberResult.get(0)
            int memberHasDeviceSize = memberDocument.field("MemberHasDeviceSize") != null ? memberDocument.field("MemberHasDeviceSize") : 0
            int memberHasIp = memberDocument.field("MemberHasIp") != null ? memberDocument.field("MemberHasIp") : 0
            int memberHasApplySize = memberDocument.field("MemberHasApplySize") != null ? memberDocument.field("MemberHasApplySize") : 0
            int memberHasOrderSize = memberDocument.field("MemberHasOrderSize") != null ? memberDocument.field("MemberHasOrderSize") : 0

            Set<String> set = []
            ODocument member = memberDocument.field("members0")
            ORidBag in_HasApply = member.field("out_MemberHasApply")
            if (null != in_HasApply && !in_HasApply.isEmpty()) {
                Iterator<OIdentifiable> it = in_HasApply.iterator()
                while (it.hasNext()) {
                    OIdentifiable t = it.next()
                    ODocument inApply = (ODocument) t
                    ODocument apply = inApply.field("in")
                    ORidBag in_HasStore = apply.field("out_ApplyHasStore")
                    if (null != in_HasStore && !in_HasStore.isEmpty()) {
                        Iterator<OIdentifiable> it1 = in_HasStore.iterator()
                        while (it1.hasNext()) {
                            ODocument inStore = (ODocument) it1.next()
                            ODocument store = inStore.field("in")
                            set.add(store.field("storeId"))
                        }
                    }
                }
            }

            ORidBag in_HasOrder = member.field("out_MemberHasOrder")
            if (null != in_HasOrder && !in_HasOrder.isEmpty()) {
                Iterator<OIdentifiable> it = in_HasOrder.iterator()
                while (it.hasNext()) {
                    OIdentifiable t = it.next()
                    ODocument inOrder = (ODocument) t
                    ODocument order = inOrder.field("in")
                    ORidBag in_HasStore = order.field("out_OrderHasStore")
                    if (null != in_HasStore && !in_HasStore.isEmpty()) {
                        Iterator<OIdentifiable> it1 = in_HasStore.iterator()
                        while (it1.hasNext()) {
                            ODocument inStore = (ODocument) it1.next()
                            ODocument store = inStore.field("in")
                            set.add(store.field("storeId"))
                        }
                    }
                }
            }

            addIndexMemberDatas(memberIndexDatas, Long.valueOf(memberId), phone, appNo, orderNo,
                    "has_device_num", memberHasDeviceSize, applyStatus, orderStatus)
            addIndexMemberDatas(memberIndexDatas, Long.valueOf(memberId), phone, appNo, orderNo,
                    "has_ip_num", memberHasIp, applyStatus, orderStatus)
            addIndexMemberDatas(memberIndexDatas, Long.valueOf(memberId), phone, appNo, orderNo,
                    "has_merchant_num", set.size(), applyStatus, orderStatus)
            addIndexMemberDatas(memberIndexDatas, Long.valueOf(memberId), phone, appNo, orderNo,
                    "has_appl_num", memberHasApplySize, applyStatus, orderStatus)
            addIndexMemberDatas(memberIndexDatas, Long.valueOf(memberId), phone, appNo, orderNo,
                    "has_order_num", memberHasOrderSize, applyStatus, orderStatus)
        }
    }

    private void addIndexMemberDatas(List<IndexData> indexDatas, long memberId, String mobile, String applyNo, String orderNo, String indexName,
                                     long direct, String applyStatus, String orderStatus) {
        IndexData indexData = new IndexData()
        indexData.setMemberId(memberId)
        indexData.setMobile(mobile)
        indexData.setDirect(direct)
        indexData.setApplyNo(applyNo)
        indexData.setOrderNo(orderNo)
        indexData.setIndexName(indexName)
        if (null != applyStatus) {
            indexData.setApplyStatus(Integer.valueOf(applyStatus))
        }
        if (null != orderStatus) {
            indexData.setOrderStatus(Integer.valueOf(orderStatus))
        }
        indexDatas.add(indexData)
    }

    private void insertMemberIndex(List<IndexData> indexDatas) {
        if (null != indexDatas) {
            def sql = "insert into member_index (member_id, apply_no, order_no,mobile,index_name,direct,create_time,apply_status,order_status) " +
                    "values(?,?,?,?,?,?,now(),?,?)"
            int indexDataSize = indexDatas.size()

            this.sql.withBatch(indexDataSize, sql) { ps ->
                for (int i = 0; i < indexDataSize; i++) {
                    ps.addBatch(indexDatas.get(i).getMemberId(), indexDatas.get(i).getApplyNo(), indexDatas.get(i).getOrderNo(),
                            indexDatas.get(i).getMobile(), indexDatas.get(i).getIndexName(), indexDatas.get(i).getDirect(),
                            indexDatas.get(i).getApplyStatus(), indexDatas.get(i).getOrderStatus())
                }
            }
        }
    }

    private void updateMemberIndex(List<IndexData> indexDatas) {
        if (null != indexDatas) {
            def sql = "update member_index set direct = ? ,update_time = now(),apply_status = ?,order_status = ? where order_no = ? and index_name = ? "
            int indexDataSize = indexDatas.size()

            this.sql.withBatch(indexDataSize, sql) { ps ->
                for (int i = 0; i < indexDataSize; i++) {
                    ps.addBatch(indexDatas.get(i).getDirect(), indexDatas.get(i).getApplyStatus(), indexDatas.get(i).getOrderStatus(),
                            indexDatas.get(i).getOrderNo(), indexDatas.get(i).getIndexName())
                }
            }
        }
    }
}
