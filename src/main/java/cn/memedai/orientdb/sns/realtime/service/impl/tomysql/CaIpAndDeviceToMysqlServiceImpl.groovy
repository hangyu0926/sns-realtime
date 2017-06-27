package cn.memedai.orientdb.sns.realtime.service.impl.tomysql

import cn.memedai.orientdb.sns.realtime.bean.IndexData
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
 * Created by hangyu on 2017/6/15.
 * 同设备客户个数、同IP客户个数，某客户不同设备数、某客户不同IP数
 */
@Service
class CaIpAndDeviceToMysqlServiceImpl {
    private static final LOG = LoggerFactory.getLogger(CaIpAndDeviceToMysqlServiceImpl.class)

    @Resource
    private OrientSql orientSql

    @Resource
    private Sql sql

    @Resource
    private ToMysqlServiceImpl toMysqlService

    @Value("#{snsOrientSqlProp.selectFromApplySql}")
    private selectFromApplySql

    @Value("#{snsOrientSqlProp.selectDeviceSql}")
    private selectDeviceSql

    @Value("#{snsOrientSqlProp.selectIpSql}")
    private selectIpSql

    @Value("#{sqlProp.selectFromApplyMysql}")
    private selectFromApplyMysql

    @Value("#{sqlProp.selectDeviceIndexSql}")
    private selectDeviceIndexSql

    @Value("#{sqlProp.selectMemberCountWhereDeviceSql}")
    private selectMemberCountWhereDeviceSql

    @Value("#{sqlProp.updateMemberApplySql}")
    private updateMemberApplySql

    void process(List<Map<String, Object>> dataList) {
        if (dataList == null || dataList.size() == 0) {
            return
        }

        Map<String, Object> applyMap = dataList.get(0)

        String appNo = applyMap.APPL_NO
        if (StringUtils.isBlank(appNo)) {
            return
        }

        String deviceId = applyMap.DEVICE_ID
        String ip = applyMap.IP

        String phone = null
        String memberId  = null
        String orderNo = null

        String op =  applyMap.__op__

        if ("update".equals(op)){
            return
        }

        //如果同设备中存在该applyNo，说明已经统计过不做操作
        if ("insert".equals(op)){
            int num = sql.firstRow(selectDeviceIndexSql,[appNo, deviceId]).num
            if (num > 0){
                return
            }
        }


        List<ODocument> list = orientSql.execute(selectFromApplySql, appNo)
        if (null != list && list.size() > 0) {
            ODocument orderNoDocument = list.get(0)
            phone = orderNoDocument.field("phone") != null ? orderNoDocument.field("phone").toString() : null
            memberId = orderNoDocument.field("memberId") != null ? orderNoDocument.field("memberId") : null
            orderNo = orderNoDocument.field("orderNo") != null ? orderNoDocument.field("orderNo").toString() : null
        }

        //如果orientDb查不到就去mysql回查
        if (null == phone){
            this.sql.rows(selectFromApplyMysql,appNo).each{
                row ->
                    memberId = row.memberId
                    phone = row.phone
                    orderNo = row.orderNo
            }
        }


        List<IndexData> deviceIndexDataList = new ArrayList<IndexData>()
        List<IndexData> ipIndexDataList = new ArrayList<IndexData>()
        toMysqlService.queryDeviceAndIpIndex(deviceIndexDataList,ipIndexDataList,memberId, phone, appNo, orderNo, deviceId,ip)
        toMysqlService.insertDeviceAndIpIndex(deviceIndexDataList,ipIndexDataList)

        //如果这个applyNo的order跑过则只需要把appNo update进去即可
        //如果order是空或者order没有先跑则做统计插入操作
        if (null != orderNo) {
            int memberCount = 0
            memberCount = sql.firstRow(selectMemberCountWhereDeviceSql, [orderNo] as Object[]).num
            if (memberCount > 0) {
                sql.execute(updateMemberApplySql, [appNo, orderNo] as Object[])
            } else {
                List<IndexData> memberIndexDatas = new ArrayList<IndexData>()
                toMysqlService.structureMemberDeviceIpIndexDatas(memberId, phone, appNo, orderNo, null, null, memberIndexDatas)
                toMysqlService.insertMemberIndex(memberIndexDatas)
            }
        } else {
            List<IndexData> memberIndexDatas = new ArrayList<IndexData>()
            toMysqlService.structureMemberDeviceIpIndexDatas(memberId, phone, appNo, orderNo, null, null, memberIndexDatas)
            toMysqlService.insertMemberIndex(memberIndexDatas)
        }
    }
}
