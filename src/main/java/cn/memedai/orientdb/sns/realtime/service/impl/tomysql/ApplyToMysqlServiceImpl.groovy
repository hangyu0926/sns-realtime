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
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service

import javax.annotation.Resource

/**
 * Created by kisho on 2017/6/8.
 * 某会员的不同申请、某会员的不同订单、某会员的不同门店
 */
@Service
class ApplyToMysqlServiceImpl implements RealTimeService {

    private static final LOG = LoggerFactory.getLogger(ApplyToMysqlServiceImpl.class)

    @Resource
    private OrientSql orientSql

    @Resource
    private Sql sql

    @Resource
    private ToMysqlServiceImpl toMysqlService

    @Value("#{snsOrientSqlProp.selectOrderFromApplySql}")
    private selectOrderFromApplySql

    @Value("#{sqlProp.selectDirectMemberCountWithOutOrderNoSql}")
    private selectDirectMemberCountWithOutOrderNoSql

    @Value("#{sqlProp.updateMemberOrderSql}")
    private updateMemberOrderSql

    @Value("#{sqlProp.selectPhoneTagCountWithOutOrderNoSql}")
    private selectPhoneTagCountWithOutOrderNoSql

    @Value("#{sqlProp.updatePhoneTagOrderSql}")
    private updatePhoneTagOrderSql

    @Value("#{sqlProp.selectPhoneTagCountWithOutMemberSql}")
    private selectPhoneTagCountWithOutMemberSql

    @Value("#{sqlProp.updatePhoneTagMemberSql}")
    private updatePhoneTagMemberSql

    @Value("#{sqlProp.selectMemberCountSql}")
    private selectMemberCountSql

    @Value("#{sqlProp.updateMemberApplySql}")
    private updateMemberApplySql

    void process(List<Map<String, Object>> dataList) {
        if (dataList == null || dataList.size() == 0) {
            return
        }

        Map<String, Object> applyMap = dataList.get(0)

        String appNo = (String) applyMap.apply_no
        if (StringUtils.isBlank(appNo)) {
            return
        }
        String phone = (String) applyMap.cellphone
        if (StringUtils.isBlank(phone)) {
            return
        }
        long memberId = applyMap.member_id
        if (StringUtils.isBlank(String.valueOf(memberId))) {
            return
        }

        String applyStatus = (String) applyMap.apply_status

        String op = applyMap.__op__
        if ("update".equals(op)) {
            return
        }

        String orderNo = null
        String orderStatus = null

        if (orderNo == null) {
            OBasicResultSet orderNoResult = orientSql.execute(selectOrderFromApplySql, appNo)
            if (null != orderNoResult && orderNoResult.size() > 0) {
                //获取到apply关联的order的no以及状态
                ODocument orderNoDocument = orderNoResult.get(0)
                orderNo = orderNoDocument.field("orderNo") != null ? orderNoDocument.field("orderNo").toString() : null
                orderStatus = orderNoDocument.field("orderStatus") != null ? orderNoDocument.field("orderStatus").toString() : null
            }
        }

        //如何申请不为空，去sns中查询是否计算过一度二度联系人指标
        if (null != appNo && null != orderNo) {
            long start = System.currentTimeMillis()
            int num = sql.firstRow(selectDirectMemberCountWithOutOrderNoSql, [appNo] as Object[]).num
            LOG.debug('{} = {} used time->{}ms',selectDirectMemberCountWithOutOrderNoSql,appNo, (System.currentTimeMillis() - start))

            if (num > 0) {
                sql.execute(updateMemberOrderSql, [orderNo, appNo] as Object[])
            }

            int phoneNum = sql.firstRow(selectPhoneTagCountWithOutOrderNoSql, [appNo] as Object[]).num
            if (phoneNum > 0) {
                sql.execute(updatePhoneTagOrderSql, [orderNo, appNo] as Object[])
            }
        }

        int memberNum = sql.firstRow(selectPhoneTagCountWithOutMemberSql, [appNo] as Object[]).num
        if (memberNum > 0) {
            //当CaCallTo在apply之前来，需要把memberId信息update进去
            sql.execute(updatePhoneTagMemberSql, [memberId, appNo] as Object[])
        }


        //如果这个applyNo的order跑过则只需要把appNo update进去即可
        //如果order是空或者order没有先跑则做统计插入操作
        if (null != orderNo) {
            int memberCount = 0
            memberCount = sql.firstRow(selectMemberCountSql, [orderNo] as Object[]).num
            if (memberCount > 0) {
                sql.execute(updateMemberApplySql, [appNo, orderNo] as Object[])
            } else {
                List<IndexData> memberIndexDatas = new ArrayList<IndexData>()
                toMysqlService.structureMemberApplyOrderIndexDatas(memberId, phone, appNo, orderNo, applyStatus, orderStatus, memberIndexDatas)
                toMysqlService.insertMemberIndex(memberIndexDatas)
            }
        } else {
            List<IndexData> memberIndexDatas = new ArrayList<IndexData>()
            toMysqlService.structureMemberApplyOrderIndexDatas(memberId, phone, appNo, orderNo, applyStatus, orderStatus, memberIndexDatas)
            toMysqlService.insertMemberIndex(memberIndexDatas)
        }
    }
}
