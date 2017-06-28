package cn.memedai.orientdb.sns.realtime.service.impl.tomysql

import cn.memedai.orientdb.sns.realtime.bean.IndexData
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import com.orientechnologies.orient.core.db.record.OIdentifiable
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.query.OBasicResultSet
import groovy.sql.Sql
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.dao.DuplicateKeyException
import org.springframework.stereotype.Service

import javax.annotation.Resource

/**
 * Created by hangyu on 2017/6/27.
 */
@Service
class ToMysqlServiceImpl{
    private static final LOG = LoggerFactory.getLogger(ToMysqlServiceImpl.class)

    @Value("#{snsOrientSqlProp.selectMemberHasApplyOrderSql}")
    private selectMemberHasApplyOrderSql

    @Value("#{snsOrientSqlProp.selectMemberHasDeviceIpSql}")
    private selectMemberHasDeviceIpSql

    @Value("#{snsOrientSqlProp.selectMemberHasSql}")
    private selectMemberHasSql

    @Value("#{sqlProp.insertMemberIndex}")
    private insertMemberIndex

    @Value("#{sqlProp.insertPhonetagIndex}")
    private insertPhonetagIndex

    @Value("#{sqlProp.insertDeviceIndex}")
    private insertDeviceIndex

    @Value("#{sqlProp.insertIpIndex}")
    private insertIpIndex

    @Value("#{snsOrientSqlProp.selectDeviceSql}")
    private selectDeviceSql

    @Value("#{snsOrientSqlProp.selectIpSql}")
    private selectIpSql

    @Resource
    private OrientSql orientSql

    @Resource
    private Sql sql

    void queryDeviceAndIpIndex(List<IndexData> deviceIndexDataList,List<IndexData> ipIndexDataList,long memberId,String phone,String appNo,String orderNo,String deviceId,String ip){
        int sameDeviceCount = 0
        OBasicResultSet deviceResult = orientSql.execute(selectDeviceSql, deviceId)
        if (null != deviceResult && deviceResult.size() > 0) {
            ODocument deviceDocument = deviceResult.get(0)
            sameDeviceCount = deviceDocument.field("MemberHasDeviceSize")
        }

        int sameIpCount = 0
        OBasicResultSet ipResult = orientSql.execute(selectIpSql, ip)
        if (null != ipResult && ipResult.size() > 0) {
            ODocument ipDocument = ipResult.get(0)
            sameIpCount= ipDocument.field("MemberHasIpSize")
        }

        addIndexDeviceIpDatas(deviceIndexDataList, Long.valueOf(memberId),phone, appNo, orderNo,
                "equal_device_member_num", sameDeviceCount, deviceId, null);

        addIndexDeviceIpDatas(ipIndexDataList, Long.valueOf(memberId), phone, appNo, orderNo,
                "equal_ip_member_num", sameIpCount, null, ip);
    }

    void structureMemberIndexDatas(long memberId,String phone,String appNo,String orderNo,String applyStatus,String orderStatus, List<IndexData> memberIndexDatas){
        OBasicResultSet memberResult = orientSql.execute(selectMemberHasSql, memberId)
        if (null != memberResult && memberResult.size() > 0) {
            ODocument memberDocument = memberResult.get(0)
            int memberHasDeviceSize = memberDocument.field("MemberHasDeviceSize") != null ? memberDocument.field("MemberHasDeviceSize") : 0
            int memberHasIp = memberDocument.field("MemberHasIpSize") != null ? memberDocument.field("MemberHasIpSize") : 0
            int memberHasApplySize = memberDocument.field("MemberHasApplySize") != null ? memberDocument.field("MemberHasApplySize") : 0
            int memberHasOrderSize = memberDocument.field("MemberHasOrderSize") != null ? memberDocument.field("MemberHasOrderSize") : 0

            Set<String> set = []
            if (null != orderNo){
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

    void structureMemberApplyOrderIndexDatas(long memberId, String phone, String appNo, String orderNo, String applyStatus, String orderStatus, List<IndexData> memberIndexDatas) {
        OBasicResultSet memberResult = orientSql.execute(selectMemberHasApplyOrderSql, memberId)
        if (null != memberResult && memberResult.size() > 0) {
            ODocument memberDocument = memberResult.get(0)
            int memberHasApplySize = memberDocument.field("MemberHasApplySize") != null ? memberDocument.field("MemberHasApplySize") : 0
            int memberHasOrderSize = memberDocument.field("MemberHasOrderSize") != null ? memberDocument.field("MemberHasOrderSize") : 0

            Set<String> set = []
            if (null != orderNo){
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
            }

            addIndexMemberDatas(memberIndexDatas, Long.valueOf(memberId), phone, appNo, orderNo,
                    "has_merchant_num", set.size(), applyStatus, orderStatus)
            addIndexMemberDatas(memberIndexDatas, Long.valueOf(memberId), phone, appNo, orderNo,
                    "has_appl_num", memberHasApplySize, applyStatus, orderStatus)
            addIndexMemberDatas(memberIndexDatas, Long.valueOf(memberId), phone, appNo, orderNo,
                    "has_order_num", memberHasOrderSize, applyStatus, orderStatus)
        }
    }

     void structureMemberDeviceIpIndexDatas(long memberId, String phone, String appNo, String orderNo, String applyStatus, String orderStatus, List<IndexData> memberIndexDatas) {
        OBasicResultSet memberResult = orientSql.execute(selectMemberHasDeviceIpSql, memberId)
        if (null != memberResult && memberResult.size() > 0) {
            ODocument memberDocument = memberResult.get(0)
            int memberHasDeviceSize = memberDocument.field("MemberHasDeviceSize") != null ? memberDocument.field("MemberHasDeviceSize") : 0
            int memberHasIp = memberDocument.field("MemberHasIpSize") != null ? memberDocument.field("MemberHasIpSize") : 0

            addIndexMemberDatas(memberIndexDatas, Long.valueOf(memberId), phone, appNo, orderNo,
                    "has_device_num", memberHasDeviceSize, applyStatus, orderStatus)
            addIndexMemberDatas(memberIndexDatas, Long.valueOf(memberId), phone, appNo, orderNo,
                    "has_ip_num", memberHasIp, applyStatus, orderStatus)
        }
    }

     void addIndexMemberDatas(List<IndexData> indexDatas, long memberId, String mobile, String applyNo, String orderNo, String indexName,
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

     void insertMemberIndex(List<IndexData> indexDatas) {
        if (null != indexDatas) {
            int indexDataSize = indexDatas.size()

            this.sql.withBatch(indexDataSize, insertMemberIndex) { ps ->
                for (int i = 0; i < indexDataSize; i++) {
                    ps.addBatch(indexDatas.get(i).getMemberId(), indexDatas.get(i).getApplyNo(), indexDatas.get(i).getOrderNo(),
                            indexDatas.get(i).getMobile(), indexDatas.get(i).getIndexName(), indexDatas.get(i).getDirect(),
                            indexDatas.get(i).getApplyStatus(), indexDatas.get(i).getOrderStatus())
                }
            }
        }
    }

     void updateMemberIndex(List<IndexData> indexDatas) {
        if (null != indexDatas) {
            def sql = "update member_index set direct = ? ,update_time = now(),apply_status = ?,order_status = ? where apply_no = ? and index_name = ? "
            int indexDataSize = indexDatas.size()

            this.sql.withBatch(indexDataSize, sql) { ps ->
                for (int i = 0; i < indexDataSize; i++) {
                    ps.addBatch(indexDatas.get(i).getDirect(), indexDatas.get(i).getApplyStatus(), indexDatas.get(i).getOrderStatus(),
                            indexDatas.get(i).getApplyNo(), indexDatas.get(i).getIndexName())
                }
            }
        }
    }

    void addIndexDatas(List<IndexData> indexDatas, long memberId, String mobile, String applyNo, String orderNo, String indexName,
                       long direct, long indirect) {
        if (null != indexName){
            IndexData indexData = new IndexData()
            indexData.setMemberId(memberId)
            indexData.setMobile(mobile)
            indexData.setDirect(direct)
            indexData.setIndirect(indirect)
            indexData.setApplyNo(applyNo)
            indexData.setOrderNo(orderNo)
            indexData.setIndexName(indexName)
            indexDatas.add(indexData)
        }
    }

    void insertPhonetagIndex(List<IndexData> indexDatas) {
        if (null != indexDatas) {
            int indexDataSize = indexDatas.size()

            this.sql.withBatch(indexDataSize, insertPhonetagIndex) { ps ->
                for (int i = 0; i < indexDataSize; i++) {
                    ps.addBatch(indexDatas.get(i).getMemberId(), indexDatas.get(i).getApplyNo(), indexDatas.get(i).getOrderNo(),
                            indexDatas.get(i).getMobile(), indexDatas.get(i).getIndexName(), indexDatas.get(i).getDirect(),
                            indexDatas.get(i).getIndirect())
                }
            }
        }
    }

    void addIndexDeviceIpDatas(List<IndexData> indexDatas, long memberId, String mobile, String applyNo, String orderNo, String indexName,
                               long direct, String deviceId, String ip) {
        if (null != indexName){
            IndexData indexData = new IndexData();
            indexData.setMemberId(memberId);
            indexData.setMobile(mobile);
            indexData.setDeviceId(deviceId);
            indexData.setIp(ip);
            indexData.setDirect(direct);
            indexData.setApplyNo(applyNo);
            indexData.setOrderNo(orderNo);
            indexData.setIndexName(indexName);
            indexDatas.add(indexData);
        }
    }

    void insertDeviceAndIpIndex (List<IndexData> deviceIndexDatas, List<IndexData> ipIndexDatas) {
        try {
            int indexDeviceDataSize = deviceIndexDatas.size()

            this.sql.withBatch(indexDeviceDataSize, insertDeviceIndex) { ps ->
                for (int i = 0; i < indexDeviceDataSize; i++) {
                    ps.addBatch(deviceIndexDatas.get(i).getMemberId(), deviceIndexDatas.get(i).getApplyNo(), deviceIndexDatas.get(i).getOrderNo(),
                            deviceIndexDatas.get(i).getMobile(),deviceIndexDatas.get(i).getDeviceId(), deviceIndexDatas.get(i).getIndexName(), deviceIndexDatas.get(i).getDirect())
                }
            }

            int indexIpDataSize = ipIndexDatas.size()

            this.sql.withBatch(indexIpDataSize, insertIpIndex) { ps ->
                for (int i = 0; i < indexIpDataSize; i++) {
                    ps.addBatch(ipIndexDatas.get(i).getMemberId(), ipIndexDatas.get(i).getApplyNo(), ipIndexDatas.get(i).getOrderNo(),
                            ipIndexDatas.get(i).getMobile(),ipIndexDatas.get(i).getIp(), ipIndexDatas.get(i).getIndexName(), ipIndexDatas.get(i).getDirect())
                }
            }
        } catch (DuplicateKeyException e) {
            LOG.error(e.toString(),e)
        }
    }

    void updateDeviceAndIpIndex (List<IndexData> deviceIndexDatas, List<IndexData> ipIndexDatas) {
        try {
            def sql = "update device_index set  direct = ? ,update_time = now() where order_no = ? and deviceId = ?"
            int indexDeviceDataSize = deviceIndexDatas.size()

            this.sql.withBatch(indexDeviceDataSize, sql) { ps ->
                for (int i = 0; i < indexDeviceDataSize; i++) {
                    ps.addBatch(deviceIndexDatas.get(i).getDirect(), deviceIndexDatas.get(i).getOrderNo(),deviceIndexDatas.get(i).getDeviceId())
                }
            }

            def ipSql = "update ip_index set  direct = ? ,update_time = now() where order_no = ? and ip = ? "

            int indexIpDataSize = ipIndexDatas.size()

            this.sql.withBatch(indexIpDataSize, ipSql) { ps ->
                for (int i = 0; i < indexIpDataSize; i++) {
                    ps.addBatch(ipIndexDatas.get(i).getDirect(), ipIndexDatas.get(i).getOrderNo(),ipIndexDatas.get(i).getIp())
                }
            }
        } catch (DuplicateKeyException e) {
            LOG.error(e.toString())
        }
    }
}
