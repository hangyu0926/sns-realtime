package cn.memedai.orientdb.sns.realtime.service.impl.tomysql

import cn.memedai.orientdb.sns.realtime.bean.IndexData
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import groovy.sql.Sql
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service

import javax.annotation.Resource

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class CashloanCashLoanOrderToMysqlServiceImpl implements RealTimeService {

    private static final LOG = LoggerFactory.getLogger(CashloanCashLoanOrderToMysqlServiceImpl.class)

    @Resource
    private OrientSql orientSql

    @Resource
    private Sql sql

    @Resource
    private ToMysqlServiceImpl toMysqlService

    @Value("#{sqlProp.selectMemberCountWhereDeviceSql}")
    private selectMemberCountWhereDeviceSql

    void process(List<Map<String, Object>> dataList) {
        if (dataList == null || dataList.size() == 0) {
            return
        }

        Map<String, Object> orderMap = dataList.get(0)

        String orderNo = (String) orderMap.order_no
        if (StringUtils.isBlank(orderNo)) {
            return
        }
        String phone = (String) orderMap.phoneNo
        if (StringUtils.isBlank(phone)) {
            return
        }
        long memberId = orderMap.member_id
        if (StringUtils.isBlank(String.valueOf(memberId))) {
            return
        }

        String op =  orderMap.__op__
        if ("update".equals(op)){
            return
        }

        String orderStatus = (String) orderMap.status

        String appNo = orderNo
        String applyStatus = null



        if ("insert".equals(op)){
            int memberCount = 0
            memberCount = sql.firstRow(selectMemberCountWhereDeviceSql,[orderNo]).num
            if (memberCount == 0){
                List<IndexData> memberIndexDatas = new ArrayList<IndexData>()
                toMysqlService.structureMemberIndexDatas(memberId,phone,appNo,orderNo,applyStatus,orderStatus,memberIndexDatas)
                toMysqlService.insertMemberIndex(memberIndexDatas)
            }
        }

    }
}
