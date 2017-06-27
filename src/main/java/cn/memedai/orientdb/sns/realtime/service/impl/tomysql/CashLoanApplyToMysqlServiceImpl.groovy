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
import org.springframework.dao.DuplicateKeyException
import org.springframework.stereotype.Service

import javax.annotation.Resource

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class CashLoanApplyToMysqlServiceImpl implements RealTimeService {

    private static final LOG = LoggerFactory.getLogger(CashLoanApplyToMysqlServiceImpl.class)

    @Resource
    private OrientSql orientSql

    @Resource
    private Sql sql

    @Resource
    private ToMysqlServiceImpl toMysqlService

    @Value("#{snsOrientSqlProp.selectDeviceSql}")
    private selectDeviceSql

    @Value("#{snsOrientSqlProp.selectIpSql}")
    private selectIpSql

    @Value("#{sqlProp.selectPhoneTagCountWithOutMemberSql}")
    private selectPhoneTagCountWithOutMemberSql

    @Value("#{sqlProp.updatePhoneTagMemberSql}")
    private updatePhoneTagMemberSql

    @Value("#{sqlProp.selectMemberCountWhereDeviceByApplySql}")
    private selectMemberCountWhereDeviceByApplySql

    @Value("#{sqlProp.selectDeviceIndexSql}")
    private selectDeviceIndexSql

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

        if ("update".equals(op)){
            return
        }

        String applyStatus = null

        String orderNo = appNo
        String orderStatus = null

        String op =  applyMap.__op__
        String deviceId = applyMap.device_id
        String ip = applyMap.ip1

        if ("insert".equals(op)){
            int memberNum = sql.firstRow(selectPhoneTagCountWithOutMemberSql, [appNo] as Object[]).num
            if (memberNum > 0) {
                //当CaCallTo在apply之前来，需要把memberId信息update进去
                sql.execute(updatePhoneTagMemberSql, [memberId, appNo] as Object[])
            }

            int memberCount = 0
            memberCount = sql.firstRow(selectMemberCountWhereDeviceByApplySql,[appNo]).num
            if (memberCount == 0){
                List<IndexData> memberIndexDatas = new ArrayList<IndexData>()
                toMysqlService.structureMemberIndexDatas(memberId,phone,appNo,orderNo,applyStatus,orderStatus,memberIndexDatas)
                toMysqlService.insertMemberIndex(memberIndexDatas)
            }

            //device,ip
            List<IndexData> deviceIndexDataList = new ArrayList<IndexData>()
            List<IndexData> ipIndexDataList = new ArrayList<IndexData>()
            toMysqlService.queryDeviceAndIpIndex(deviceIndexDataList,ipIndexDataList,memberId, phone, appNo, orderNo, deviceId,ip)
            toMysqlService.insertDeviceAndIpIndex(deviceIndexDataList,ipIndexDataList)
        }
    }


}
