package cn.memedai.orientdb.sns.realtime.service.impl.tomysql

import cn.memedai.orientdb.sns.realtime.bean.IndexData
import cn.memedai.orientdb.sns.realtime.sql.OrientSql
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
 * Created by hangyu on 2017/6/15.
 */
@Service
class CtaIpAndDeviceToMysqlServiceImpl {
    private static final LOG = LoggerFactory.getLogger(CtaIpAndDeviceToMysqlServiceImpl.class)

    @Resource
    private OrientSql orientSql

    @Resource
    private Sql sql

    @Resource
    private ToMysqlServiceImpl toMysqlService

    @Value("#{snsOrientSqlProp.selectFromOrderSql}")
    private selectFromOrderSql

    @Value("#{snsOrientSqlProp.selectDeviceSql}")
    private selectDeviceSql

    @Value("#{snsOrientSqlProp.selectIpSql}")
    private selectIpSql

    @Value("#{sqlProp.selectFromOrderMysql}")
    private selectFromOrderMysql

    @Value("#{sqlProp.selectDeviceIndexByOrderSql}")
    private selectDeviceIndexByOrderSql

    @Value("#{sqlProp.selectMemberCountWhereDeviceSql}")
    private selectMemberCountWhereDeviceSql

    @Value("#{sqlProp.selectMemberCountWhereDeviceByApplySql}")
    private selectMemberCountWhereDeviceByApplySql

    @Value("#{sqlProp.updateMemberOrderSql}")
    private updateMemberOrderSql

    void process(List<Map<String, Object>> dataList) {
        if (dataList == null || dataList.size() == 0) {
            return
        }

        Map<String, Object> applyMap = dataList.get(0)

        String orderNo = (String) applyMap.ORDER_ID
        if (StringUtils.isBlank(orderNo)) {
            return
        }

        String deviceId = applyMap.DEVICE_ID
        String ip = applyMap.IP
        String phone = null
        String memberId  = null
        String appNo = null

        String op =  applyMap.__op__

        OBasicResultSet orderNoResult = orientSql.execute(selectFromOrderSql, orderNo)
        if (null != orderNoResult && orderNoResult.size() > 0) {
            ODocument orderNoDocument = orderNoResult.get(0)
            phone = orderNoDocument.field("phone") != null ? orderNoDocument.field("phone").toString() : null
            memberId = orderNoDocument.field("memberId") != null ? orderNoDocument.field("memberId") : null
            appNo = orderNoDocument.field("applyNo") != null ? orderNoDocument.field("applyNo").toString() : null
        }

        //如果orientDb查不到就去mysql回查
        if (null == phone){
            sql.rows(selectFromOrderMysql,orderNo).each{
                row ->
                    memberId = row.memberId
                    phone = row.phone
                    appNo = row.applyNo
            }
        }

        //如果同设备中存在该orderNo，说明已经统计过不做操作
        int num = sql.firstRow(selectDeviceIndexByOrderSql,[orderNo, deviceId]).num
        if (num == 0){
            List<IndexData> deviceIndexDataList = new ArrayList<IndexData>()
            List<IndexData> ipIndexDataList = new ArrayList<IndexData>()
            toMysqlService.queryDeviceAndIpIndex(deviceIndexDataList,ipIndexDataList,Long.valueOf(memberId), phone, appNo, orderNo, deviceId,ip)
            toMysqlService.insertDeviceAndIpIndex(deviceIndexDataList,ipIndexDataList)
        }



        //如果这个orderNo的apply跑过则只需要把orderNo update进去即可
        //如果appNo是空或者apply没有先跑则做统计插入操作

        int memberCount = 0 //查询此orderNo是否跑过，防止重复跑
        memberCount = sql.firstRow(selectMemberCountWhereDeviceSql, [appNo] as Object[]).num

        if (null != appNo) {
            int memberCountByApply = 0
            memberCountByApply = sql.firstRow(selectMemberCountWhereDeviceByApplySql, [appNo] as Object[]).num
            if (memberCountByApply > 0) {
                sql.execute(updateMemberOrderSql, [orderNo, appNo] as Object[])
            } else {
                if (memberCount == 0){
                    List<IndexData> memberIndexDatas = new ArrayList<IndexData>()
                    toMysqlService.structureMemberDeviceIpIndexDatas(Long.valueOf(memberId), phone, appNo, orderNo, null, null, memberIndexDatas)
                    toMysqlService.insertMemberIndex(memberIndexDatas)
                }
            }
        } else {
            if (memberCount == 0){
                List<IndexData> memberIndexDatas = new ArrayList<IndexData>()
                toMysqlService.structureMemberDeviceIpIndexDatas(Long.valueOf(memberId), phone, appNo, orderNo, null, null, memberIndexDatas)
                toMysqlService.insertMemberIndex(memberIndexDatas)
            }
        }
    }
}
