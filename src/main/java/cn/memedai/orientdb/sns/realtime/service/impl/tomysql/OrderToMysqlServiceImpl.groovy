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
class OrderToMysqlServiceImpl implements RealTimeService {

    private static final LOG = LoggerFactory.getLogger(OrderToMysqlServiceImpl.class)

    @Resource
    private OrientSql orientSql

    @Resource
    private Sql sql

    @Resource
    private ToMysqlServiceImpl toMysqlService

    @Value("#{snsOrientSqlProp.selectApplyFromOrderSql}")
    private selectApplyFromOrderSql

    @Value("#{sqlProp.selectMemberCountSql}")
    private selectMemberCountSql

    void process(List<Map<String, Object>> dataList) {
        if (dataList == null || dataList.size() == 0) {
            return
        }

        Map<String, Object> orderMap = dataList.get(0)

        String op = orderMap.__op__
        if ("update".equals(op)) {
            return
        }

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

        OBasicResultSet orderNoResult = orientSql.execute(selectApplyFromOrderSql, orderNo)
        if (null != orderNoResult && orderNoResult.size() > 0) {
            ODocument orderNoDocument = orderNoResult.get(0)
            appNo = orderNoDocument.field("applyNo") != null ? orderNoDocument.field("applyNo").toString() : null
            applyStatus = orderNoDocument.field("applyStatus") != null ? orderNoDocument.field("applyStatus").toString() : null
        }

        //如果存在appNo说明apply已经先来了不需要做任何操作,因为关系是在apply的service中新建的
        //如果不存在说明Order先来或者压根没有apply都只要做统计插入即可
        if (null == appNo) {
            int memberCount = 0 //查询此orderNo是否跑过，防止重复跑
            memberCount = sql.firstRow(selectMemberCountSql, [orderNo] as Object[]).num
            if (memberCount == 0){
                List<IndexData> memberIndexDatas = new ArrayList<IndexData>()
                toMysqlService.structureMemberApplyOrderIndexDatas(memberId, phone, appNo, orderNo, applyStatus, orderStatus, memberIndexDatas)
                toMysqlService.insertMemberIndex(memberIndexDatas)
            }
        }
    }
}
