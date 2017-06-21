package cn.memedai.orientdb.sns.realtime.service.impl.tomysql

import cn.memedai.orientdb.sns.realtime.bean.IndexData
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import com.orientechnologies.orient.core.db.record.OIdentifiable
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.query.OBasicResultSet
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.BatchPreparedStatementSetter
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.PreparedStatementSetter
import org.springframework.stereotype.Service

import javax.annotation.Resource
import java.sql.PreparedStatement
import java.sql.SQLException

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class OrderToMysqlServiceImpl implements RealTimeService {

    private static final LOG = LoggerFactory.getLogger(OrderToMysqlServiceImpl.class)

    @Resource
    private OrientSql orientSql

    @Resource
    private JdbcTemplate jdbcTemplate

    private selectOrderFromApplySql = 'select in("ApplyHasOrder").applyNo as applyNo,in("ApplyHasOrder").originalStatus as applyStatus from order where orderNo = ? unwind applyNo,applyStatus'

    private selectMemberSql = 'select out("MemberHasDevice").size() as MemberHasDeviceSize,out("MemberHasIp").size() as MemberHasIpSize,' +
            'out("MemberHasApply").size() as MemberHasApplySize,out("MemberHasOrder").size() as MemberHasOrderSize,@rid as members0 from member where memberId = ?'

    private selectApplyCountSql = 'SELECT id FROM member_index where apply_no = ? and order_no is null'

    private updateMemberOrderSql ='update member_index set order_no = ? where apply_no = ?'

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

        String orderStatus = (String)orderMap.status

        String appNo = null
        String applyStatus = null

        if (appNo == null){
            OBasicResultSet orderNoResult = orientSql.execute(selectOrderFromApplySql,orderNo)
            if (null != orderNoResult){
                ODocument orderNoDocument = orderNoResult.get(0)
                appNo =  orderNoDocument.field("applyNo") != null ? orderNoDocument.field("applyNo").toString() : null
                applyStatus = orderNoDocument.field("applyStatus") != null ? orderNoDocument.field("applyStatus").toString() : null
            }
        }

        //如何申请不为空，去sns中查询是否计算过一度二度联系人指标
        if (null != appNo){
            List<Map<String, Object>> list = jdbcTemplate.queryForList(selectApplyCountSql,appNo)
            if (list.size() > 0){
                jdbcTemplate.update(updateMemberOrderSql, new PreparedStatementSetter(){
                    @Override
                     void setValues(PreparedStatement ps) throws SQLException {
                        ps.setString(1, orderNo);
                        ps.setString(2, appNo);
                    }
                })
            }
        }


        OBasicResultSet memberResult =  orientSql.execute(selectMemberSql,memberId)
        if (null != memberResult){
            ODocument memberDocument = memberResult.get(0)
            int memberHasDeviceSize =memberDocument.field("MemberHasDeviceSize") != null ? memberDocument.field("MemberHasDeviceSize") : 0
            int memberHasIp =memberDocument.field("MemberHasIp") != null ? memberDocument.field("MemberHasIp") : 0
            int memberHasApplySize =memberDocument.field("MemberHasApplySize") != null ? memberDocument.field("MemberHasApplySize") : 0
            int memberHasOrderSize =memberDocument.field("MemberHasOrderSize") != null ? memberDocument.field("MemberHasOrderSize") : 0

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

            List<IndexData> memberIndexDatas = new ArrayList<IndexData>()

            addIndexMemberDatas(memberIndexDatas,  Long.valueOf(memberId), phone, appNo, orderNo,
                    "has_device_num", memberHasDeviceSize,applyStatus,orderStatus)
            addIndexMemberDatas(memberIndexDatas,  Long.valueOf(memberId), phone, appNo, orderNo,
                    "has_ip_num", memberHasIp,applyStatus,orderStatus)
            addIndexMemberDatas(memberIndexDatas,  Long.valueOf(memberId), phone, appNo, orderNo,
                    "has_merchant_num", set.size(),applyStatus,orderStatus)
            addIndexMemberDatas(memberIndexDatas, Long.valueOf(memberId), phone, appNo, orderNo,
                    "has_appl_num",memberHasApplySize,applyStatus,orderStatus)
            addIndexMemberDatas(memberIndexDatas, Long.valueOf(memberId), phone, appNo, orderNo,
                    "has_order_num", memberHasOrderSize,applyStatus,orderStatus)

            insertMemberIndex(memberIndexDatas)
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
        if (null != applyStatus){
            indexData.setApplyStatus(Integer.valueOf(applyStatus))
        }
        if (null != orderStatus){
            indexData.setOrderStatus(Integer.valueOf(orderStatus))
        }
        indexDatas.add(indexData)
    }

    private void insertMemberIndex(List<IndexData> indexDatas) {
        if (null != indexDatas) {
            def sql = "insert into member_index (member_id, apply_no, order_no,mobile,index_name,direct,create_time,apply_status,order_status) " +
                    "values(?,?,?,?,?,?,now(),?,?)"

            int indexDataSize = indexDatas.size()
            jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                int getBatchSize() {
                    return indexDataSize
                }
                void setValues(PreparedStatement ps, int i)throws SQLException {
                    IndexData indexData = indexDatas.get(i)
                    ps.setLong(1, indexData.getMemberId())
                    ps.setString(2, indexData.getApplyNo())
                    ps.setString(3, indexData.getOrderNo())
                    ps.setString(4, indexData.getMobile())
                    ps.setString(5, indexData.getIndexName())
                    ps.setLong(6, indexData.getDirect())
                    ps.setInt(7, indexData.getApplyStatus())
                    ps.setInt(8, indexData.getOrderStatus())
                }
            })
        }
    }
}
