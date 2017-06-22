package cn.memedai.orientdb.sns.realtime.service.impl.tomysql

import cn.memedai.orientdb.sns.realtime.bean.IndexData
import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.query.OBasicResultSet
import groovy.sql.Sql
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.dao.DuplicateKeyException
import org.springframework.jdbc.core.BatchPreparedStatementSetter
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service

import javax.annotation.Resource
import java.sql.PreparedStatement
import java.sql.SQLException

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
    private JdbcTemplate jdbcTemplate

    private selectFromOrderOrientSql = 'select in("PhoneHasOrder").phone as phone,in("MemberHasOrder").memberId as memberId,in("ApplyHasOrder").applyNo as applyNo from order where orderNo = ? unwind phone,memberId,applyNo'

    private selectFromOrderMysql = 'select a.member_id as memberId,a.mobile as phone,b.apply_no as applyNo from network.money_box_order a left join network.apply_info b on a.order_no = b.order_no where a.order_no = ?'

    private selectDeviceIndexSql = 'SELECT id FROM device_index where order_no = ? and deviceId = ?'

    private selectDeviceSql = 'select @rid as device0 from device where deviceId = ?'

    private selectIpSql = 'select @rid as ip0 from ip where ip = ?'

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

        //如果同设备中存在该orderNo，说明已经统计过不做操作
        List<Map<String, Object>> list = jdbcTemplate.queryForList(selectDeviceIndexSql, orderNo,deviceId)
        if (list != null && list.size() > 0) {
            return
        }

        OBasicResultSet orderNoResult = orientSql.execute(selectFromOrderOrientSql, orderNo)
        if (null != orderNoResult && orderNoResult.size() > 0) {
            ODocument orderNoDocument = orderNoResult.get(0)
            phone = orderNoDocument.field("phone") != null ? orderNoDocument.field("phone").toString() : null
            memberId = orderNoDocument.field("memberId") != null ? orderNoDocument.field("memberId") : null
            appNo = orderNoDocument.field("applyNo") != null ? orderNoDocument.field("applyNo").toString() : null
        }

        //如果orientDb查不到就去mysql回查
        if (null == phone || null == memberId){
            jdbcTemplate.queryForList(selectFromOrderMysql,orderNo).each {
                row ->
                    memberId = row.memberId
                    phone = row.phone
                    appNo = row.applyNo
            }
        }

        int sameDeviceCount = 0
        OBasicResultSet deviceResult = orientSql.execute(selectDeviceSql, deviceId)
        if (null != deviceResult && deviceResult.size() > 0) {
            ODocument deviceDocument = deviceResult.get(0)
            ODocument device = deviceDocument.field("device0")
            ORidBag out_HasDevice = device.field("in_MemberHasDevice")
            if (null != out_HasDevice){
                sameDeviceCount = out_HasDevice.size()
            }
        }

        int sameIpCount = 0
        OBasicResultSet ipResult = orientSql.execute(selectIpSql, ip)
        if (null != ipResult && ipResult.size() > 0) {
            ODocument ipDocument = ipResult.get(0)
            ODocument ipD = ipDocument.field("ip0")
            ORidBag out_HasIp = ipD.field("in_MemberHasIp")
            if (null != out_HasIp){
                sameIpCount = out_HasIp.size()
            }
        }

        List<IndexData> deviceIndexDataList = new ArrayList<IndexData>()
        addIndexDatas(deviceIndexDataList, Long.valueOf(memberId),phone, appNo, orderNo,
                "equal_device_member_num", sameDeviceCount, deviceId, null);

        List<IndexData> ipIndexDataList = new ArrayList<IndexData>()
        addIndexDatas(ipIndexDataList, Long.valueOf(memberId), phone, appNo, orderNo,
                "equal_ip_member_num", sameIpCount, null, ip);

        //如果同设备中存在该orderNo，说明已经统计过不做操作
        List<Map<String, Object>> deviceList = jdbcTemplate.queryForList(selectDeviceIndexSql, orderNo,deviceId)
        if (deviceList != null && deviceList.size() > 0) {
            return
        }
        insertDeviceAndIpIndex(deviceIndexDataList,ipIndexDataList)
    }

    void insertDeviceAndIpIndex (List<IndexData> deviceIndexDatas, List<IndexData> ipIndexDatas) {
        try {
            def sql = "insert into device_index (member_id, apply_no, order_no,mobile,deviceId,index_name,direct,create_time) " +
                    " values(?,?,?,?,?,?,?,now())"

            int indexDataSize = deviceIndexDatas.size()
            jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                int getBatchSize() {
                    return indexDataSize
                }

                void setValues(PreparedStatement ps, int i) throws SQLException {
                    IndexData indexData = deviceIndexDatas.get(i)
                    ps.setLong(1, indexData.getMemberId())
                    ps.setString(2, indexData.getApplyNo())
                    ps.setString(3, indexData.getOrderNo())
                    ps.setString(4, indexData.getMobile())
                    ps.setString(5, indexData.getDeviceId())
                    ps.setString(6, indexData.getIndexName())
                    ps.setLong(7, indexData.getDirect())
                }
            })

            def ipSql = "insert into ip_index (member_id, apply_no, order_no,mobile,ip,index_name,direct,create_time) " +
                    " values(?,?,?,?,?,?,?,now())"

            int indexIpDataSize = ipIndexDatas.size()
            jdbcTemplate.batchUpdate(ipSql, new BatchPreparedStatementSetter() {
                int getBatchSize() {
                    return indexIpDataSize
                }

                void setValues(PreparedStatement ps, int i) throws SQLException {
                    IndexData indexData = ipIndexDatas.get(i)
                    ps.setLong(1, indexData.getMemberId())
                    ps.setString(2, indexData.getApplyNo())
                    ps.setString(3, indexData.getOrderNo())
                    ps.setString(4, indexData.getMobile())
                    ps.setString(5, indexData.getIp())
                    ps.setString(6, indexData.getIndexName())
                    ps.setLong(7, indexData.getDirect())
                }
            })
        } catch (DuplicateKeyException e) {
            LOG.error(e.toString())
        }
    }

    void addIndexDatas(List<IndexData> indexDatas, long memberId, String mobile, String applyNo, String orderNo, String indexName,
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
}
