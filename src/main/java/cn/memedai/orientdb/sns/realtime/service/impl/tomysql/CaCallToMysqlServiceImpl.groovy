package cn.memedai.orientdb.sns.realtime.service.impl.tomysql

import cn.memedai.orientdb.sns.realtime.bean.ConstantHelper
import cn.memedai.orientdb.sns.realtime.bean.IndexData
import cn.memedai.orientdb.sns.realtime.bean.IndexNameEnum
import cn.memedai.orientdb.sns.realtime.bean.MemberDeviceAndApplyAndOrderBean
import cn.memedai.orientdb.sns.realtime.cache.ApplyCache
import cn.memedai.orientdb.sns.realtime.cache.ApplyNoOrderNoCache
import cn.memedai.orientdb.sns.realtime.cache.ApplyRidPhoneRidCache
import cn.memedai.orientdb.sns.realtime.cache.PhoneCache
import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import cn.memedai.orientdb.sns.realtime.util.DateUtils
import com.orientechnologies.orient.core.db.record.OIdentifiable
import com.orientechnologies.orient.core.db.record.ORecordLazyList
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.query.OBasicResultSet
import com.orientechnologies.orient.core.sql.query.OResultSet
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.BatchPreparedStatementSetter
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service

import javax.annotation.Resource
import java.sql.PreparedStatement
import java.sql.SQLException

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class CaCallToMysqlServiceImpl implements RealTimeService {

    private static final LOG = LoggerFactory.getLogger(CaCallToMysqlServiceImpl.class)

    @Resource
    private OrientSql orientSql

    @Resource
    private JdbcTemplate jdbcTemplate

    @Resource
    private ApplyCache applyCache

    @Resource
    private ApplyRidPhoneRidCache applyRidPhoneRidCache

    @Resource
    private PhoneCache phoneCache

    private selectPhoneFromApplySql = 'select in("PhoneHasApply").phone as phone,originalStatus as applyStatus from apply where applyNo = ? unwind phone,applyStatus'

    private selectOrderFromApplySql = 'select out("ApplyHasOrder").orderNo as orderNo,out("ApplyHasOrder").originalStatus as orderStatus from apply where applyNo = ? unwind orderNo,orderStatus'

    private selectCallToFromPhoneSql = 'select @rid as phoneRid0,phone as phone, unionall(in_CallTo,out_CallTo) as callTos,in("HasPhone") as members0 from Phone where phone = ?'

    @Resource
    private ApplyNoOrderNoCache applyNoOrderNoCache

    void process(List<Map<String, Object>> dataList) {
        if (dataList == null || dataList.size() == 0) {
            return
        }

        Map<String, Object> callToMap = dataList.get(0)

        String appNo = (String) callToMap.APPL_NO

        if (StringUtils.isBlank(appNo)) {
            return
        }


        OBasicResultSet phoneResult = orientSql.execute(selectPhoneFromApplySql,appNo)
        ODocument phoneDocument = phoneResult.get(0)
        String phone =phoneDocument.field("phone") != null ? phoneDocument.field("phone").toString() : null
        String applyStatus = phoneDocument.field("applyStatus") != null ? phoneDocument.field("applyStatus").toString() : null
        if (StringUtils.isBlank(phone)){
            return
        }

        String orderNo = null
        String orderStatus = null
        /*CacheEntry applyNoOrderNoCacheEntry = applyNoOrderNoCache.get(appNo)
        if (applyNoOrderNoCacheEntry != null) {
            orderNo = applyNoOrderNoCacheEntry.value
        }*/

        //如果缓存中没有再去查询
        if (orderNo == null){
            OBasicResultSet orderNoResult = orientSql.execute(selectOrderFromApplySql,appNo)
            if (null != orderNoResult){
                ODocument orderNoDocument = orderNoResult.get(0)
                orderNo =  orderNoDocument.field("orderNo") != null ? orderNoDocument.field("orderNo").toString() : null
                orderStatus = orderNoDocument.field("orderStatus") != null ? orderNoDocument.field("orderStatus").toString() : null
            }
        }

        //查询一度二度
        HashMap<String, Integer> map = [:]
        HashMap<String, Integer> map2 = [:]
        OResultSet phoneInfos = orientSql.execute(selectCallToFromPhoneSql,phone)
        if (phoneInfos == null || phoneInfos.size() <= 0) {
            return
        }

        ODocument phoneInfo = (ODocument) phoneInfos.get(0)
        ORecordLazyList members0 = phoneInfo.field("members0")

        if (members0.isEmpty()){
            return
        }
        ODocument member = (ODocument) members0.get(0)
        String memberId = member.field("memberId")

        MemberDeviceAndApplyAndOrderBean memberDeviceAndApplyAndOrderBean = new MemberDeviceAndApplyAndOrderBean()

        long start = System.currentTimeMillis()
        queryDirectRelationDataByPhoneNo(phone,map,map2,phoneInfo,memberDeviceAndApplyAndOrderBean)
        LOG.debug('queryDirectRelationDataByPhoneNo used time->{}ms',  (System.currentTimeMillis() - start))

        //插入一度和二度联系人指标开始
        List<IndexData> indexDatas = new ArrayList<IndexData>()
        Set<Map.Entry<String, Integer>> directSet = map.entrySet()
        List<String> directMarks = new ArrayList<String>()

        for (Map.Entry<String, Integer> en : directSet) {
            addIndexDatas(indexDatas, Long.valueOf(memberId), phone, appNo, orderNo,
                    IndexNameEnum.fromValue(en.getKey()), en.getValue(), 0)
            directMarks.add(en.getKey())
        }

        Set<Map.Entry<String, Integer>> indirectResultSet = map2.entrySet()
        for (Map.Entry<String, Integer> en : indirectResultSet) {
            if (directMarks.contains(en.getKey())) {
                for (IndexData indexData : indexDatas) {
                    if (indexData.getIndexName().equals(IndexNameEnum.fromValue(en.getKey()))) {
                        indexData.setIndirect(en.getValue())
                    }
                }
            } else {
                addIndexDatas(indexDatas, Long.valueOf(memberId), phone, appNo, orderNo,
                        IndexNameEnum.fromValue(en.getKey()), 0, en.getValue())
            }

        }

        insertPhonetagIndex(indexDatas)

        List<IndexData> memberIndexDatas = new ArrayList<IndexData>()

        addIndexMemberDatas(memberIndexDatas,  Long.valueOf(memberId), phone, appNo, orderNo,
                "contact_accept_member_num", memberDeviceAndApplyAndOrderBean.getContactAcceptMemberNum(),applyStatus,orderStatus)
        addIndexMemberDatas(memberIndexDatas,  Long.valueOf(memberId), phone, appNo, orderNo,
                "contact_refuse_member_num", memberDeviceAndApplyAndOrderBean.getContactRefuseMemberNum(),applyStatus,orderStatus)
        addIndexMemberDatas(memberIndexDatas,  Long.valueOf(memberId), phone, appNo, orderNo,
                "contact_overdue_member_num", memberDeviceAndApplyAndOrderBean.getContactOverdueMemberNum(),applyStatus,orderStatus)
        addIndexMemberDatas(memberIndexDatas, Long.valueOf(memberId), phone, appNo, orderNo,
                "contact_black_member_num", memberDeviceAndApplyAndOrderBean.getContactBlackMemberNum(),applyStatus,orderStatus)

        addIndexMemberDatas(memberIndexDatas, Long.valueOf(memberId), phone, appNo, orderNo,
                "contact_accept_member_120s_num", memberDeviceAndApplyAndOrderBean.getContactAcceptMemberCallLenNum(),applyStatus,orderStatus)
        addIndexMemberDatas(memberIndexDatas, Long.valueOf(memberId), phone, appNo, orderNo,
                "contact_refuse_member_120s_num", memberDeviceAndApplyAndOrderBean.getContactRefuseMemberCallLenNum(),applyStatus,orderStatus)
        addIndexMemberDatas(memberIndexDatas,  Long.valueOf(memberId), phone, appNo, orderNo,
                "contact_overdue_member_120s_num", memberDeviceAndApplyAndOrderBean.getContactOverdueMemberCallLenNum(),applyStatus,orderStatus)
        addIndexMemberDatas(memberIndexDatas,  Long.valueOf(memberId), phone, appNo, orderNo,
                "contact_black_member_120s_num", memberDeviceAndApplyAndOrderBean.getContactBlackMemberCallLenNum(),applyStatus,orderStatus)

        insertMemberIndex(memberIndexDatas)
    }

    private void queryDirectRelationDataByPhoneNo(String memberRelatedPhoneNo,Map<String, Integer> map, Map<String, Integer> map2,ODocument phoneInfo,MemberDeviceAndApplyAndOrderBean memberDeviceAndApplyAndOrderBean){
        ODocument phoneRecord0 = phoneInfo.field("phoneRid0")
        ORecordLazyList ocrs = phoneInfo.field("callTos")
        Map<String, String> tempMap = [:]
        Map<String, String> tempCallLenMap = [:]
        List<String> directPhones = []

        //一度联系人过件个数
        int contactAccept = 0
        //一度联系人拒件个数
        int contactRefuse = 0
        //一度联系人逾期个数
        int contactOverdue = 0
        //一度联系人黑名单个数
        int contactBlack = 0

        //一度联系人过件个数
        int contactAcceptCallLen = 0
        //一度联系人拒件个数
        int contactRefuseCallLen = 0
        //一度联系人逾期个数
        int contactOverdueCallLen = 0
        //一度联系人黑名单个数
        int contactBlackCallLen = 0

        Map<String, String> hasdirectMap = [:]
        Map<String, String> hasCallLendirectMap = [:]
        if (ocrs != null && !ocrs.isEmpty()) {
            int ocrSize = ocrs.size()
            for (int j = 0; j < ocrSize; j++) {
                ODocument ocr = (ODocument) ocrs.get(j)
                //一度联系人的通话时长
                //通话时长
                Object directCallLen = ocr.field("callLen")
                int directCallLength = 0
                if (directCallLen instanceof String) {
                    directCallLength = Integer.valueOf((String) directCallLen)
                } else {
                    directCallLength = (Integer) directCallLen
                }

                ODocument tempPhoneRecordIn1 = ocr.field("in")//callTo边
                ODocument tempPhoneRecordOut1 = ocr.field("out")
                //设置一级联系人的record
                ODocument phoneRecord1 = getRid(tempPhoneRecordIn1).equals(getRid(phoneRecord0)) ? tempPhoneRecordOut1 : tempPhoneRecordIn1//phone点

                String phone = phoneRecord1.field("phone").toString()

                //对一度联系人phone做下校验
                if (directPhones.contains(phone) || memberRelatedPhoneNo.equals(phone)) {
                    continue
                }
                directPhones.add(phone)


                //查询二度开始
                if (checkPhone(phone)) {
                    ORidBag inCallTo = phoneRecord1.field("in_CallTo")
                    if (null != inCallTo && !inCallTo.isEmpty()) {
                        Iterator<OIdentifiable> it = inCallTo.iterator()
                        while (it.hasNext()) {
                            OIdentifiable t = it.next()
                            ODocument inphone = (ODocument) t
                            if (null == inphone) {
                                LOG.error("in_CallTo is null ,this phone is {}", phone)
                                continue
                            }
                            ODocument phone1 = inphone.field("out")
                            //通话时长
                            Object callLen = inphone.field("callLen")
                            int callLength = 0
                            if (callLen instanceof String) {
                                callLength = Integer.valueOf((String) callLen)
                            } else {
                                callLength = (Integer) callLen
                            }
                            String indirectphone = phone1.field("phone")
                            if (!memberRelatedPhoneNo.equals(indirectphone)) {
                                ORidBag outHasPhoneMark = phone1.field("out_HasPhoneMark")
                                if (null != outHasPhoneMark && !outHasPhoneMark.isEmpty()) {
                                    Iterator<OIdentifiable> it1 = outHasPhoneMark.iterator()
                                    while (it1.hasNext()) {
                                        OIdentifiable t1 = it1.next()
                                        ODocument phoneMark = (ODocument) t1
                                        ODocument phoneMark1 = phoneMark.field("in")
                                        String mark = phoneMark1.field("mark")
                                        tempMap.put(indirectphone, mark)

                                        if (callLength >= ConstantHelper.CALL_LEN) {
                                            tempCallLenMap.put(indirectphone, mark + ConstantHelper.MARK_CALL_LEN)
                                        }
                                    }
                                }
                            }
                        }
                    }

                    ORidBag outCallTo = phoneRecord1.field("out_CallTo")
                    if (null != outCallTo && !outCallTo.isEmpty()) {
                        Iterator<OIdentifiable> it = outCallTo.iterator()
                        while (it.hasNext()) {
                            OIdentifiable t = it.next()
                            ODocument outphone = (ODocument) t
                            if (null == outphone) {
                                LOG.error("outphone is null {}", outCallTo.toString())
                                continue
                            }
                            ODocument phone1 = outphone.field("in")
                            //通话时长
                            Object callLen = outphone.field("callLen")
                            int callLength = 0
                            if (callLen instanceof String) {
                                callLength = Integer.valueOf((String) callLen)
                            } else {
                                callLength = (Integer) callLen
                            }
                            String indirectphone = phone1.field("phone")
                            if (!memberRelatedPhoneNo.equals(indirectphone)) {
                                ORidBag outHasPhoneMark = phone1.field("out_HasPhoneMark")
                                if (null != outHasPhoneMark && !outHasPhoneMark.isEmpty()) {
                                    Iterator<OIdentifiable> it1 = outHasPhoneMark.iterator()
                                    while (it1.hasNext()) {
                                        OIdentifiable t1 = it1.next()
                                        ODocument phoneMark = (ODocument) t1
                                        ODocument phoneMark1 = phoneMark.field("in")
                                        String mark = phoneMark1.field("mark")
                                        tempMap.put(indirectphone, mark)

                                        if (callLength >= ConstantHelper.CALL_LEN) {
                                            tempCallLenMap.put(indirectphone, mark + ConstantHelper.MARK_CALL_LEN)
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                //查询二度结束

                //统计一度电话标签数据map
                ORidBag outHasPhoneMark = phoneRecord1.field("out_HasPhoneMark")//HasPhoneMark边

                if (null != outHasPhoneMark && !outHasPhoneMark.isEmpty()) {
                    Iterator<OIdentifiable> it = outHasPhoneMark.iterator()
                    while (it.hasNext()) {
                        OIdentifiable t = it.next()
                        ODocument phoneMark = (ODocument) t
                        ODocument phoneMark1 = phoneMark.field("in")
                        String mark = phoneMark1.field("mark")

                        hasdirectMap.put(phone, mark)

                        if (map.containsKey(mark)) {
                            Integer count = map.get(mark) + 1
                            map.put(mark, count)
                        } else {
                            map.put(mark, 1)
                        }

                        //判断通话时长超过120S
                        if (directCallLength >= ConstantHelper.CALL_LEN) {
                            String callMark = mark + ConstantHelper.MARK_CALL_LEN
                            hasCallLendirectMap.put(phone, callMark)
                            if (map.containsKey(callMark)) {
                                Integer count = map.get(callMark) + 1
                                map.put(callMark, count)
                            } else {
                                map.put(callMark, 1)
                            }
                        }

                    }
                }

                //查询过件、拒件、逾期、黑名单
                ORidBag in_HasPhone = phoneRecord1.field("in_HasPhone")
                if (null != in_HasPhone && !in_HasPhone.isEmpty()) {
                    Iterator<OIdentifiable> it = in_HasPhone.iterator()
                    while (it.hasNext()) {
                        OIdentifiable t = it.next()
                        ODocument member = (ODocument) t
                        ODocument member1 = member.field("out")
                        Boolean isBlack = member1.field("isBlack") == null ? false : (Boolean) member1.field("isBlack")
                        Boolean isOverdue = member1.field("isOverdue") == null ? false : (Boolean) member1.field("isOverdue")

                        if (isBlack) {
                            contactBlack++
                        }
                        if (isOverdue) {
                            contactOverdue++
                        }

                        //判断通话时长超过120S
                        if (directCallLength >= ConstantHelper.CALL_LEN) {
                            if (isBlack) {
                                contactBlackCallLen++
                            }
                            if (isOverdue) {
                                contactOverdueCallLen++
                            }
                        }

                        long memberId = member1.field("memberId")

                        String originalStatus = null

                        ORidBag out_MemberHasOrder = member1.field("out_MemberHasOrder")
                        if (null != out_MemberHasOrder && !out_MemberHasOrder.isEmpty()) {
                            long lastTime = 0
                            Iterator<OIdentifiable> it1 = out_MemberHasOrder.iterator()
                            while (it1.hasNext()) {
                                OIdentifiable t1 = it1.next()
                                ODocument order = (ODocument) t1
                                ODocument order1 = order.field("in")
                                Date createdDatetime = order1.field("createdDatetime")
                                long stringToLong = 0
                                stringToLong = DateUtils.dateToLong(createdDatetime)
                                if (stringToLong > lastTime) {
                                    lastTime = stringToLong
                                    originalStatus = order1.field("originalStatus")
                                }
                            }
                        }
                        if (ConstantHelper.REFUSE_APPLY_FLAG.equals(originalStatus)) {
                            contactRefuse++
                        } else if (ConstantHelper.PASS_APPLY_FLAG.equals(originalStatus)) {
                            contactAccept++
                        }

                        //判断通话时长超过120S
                        if (directCallLength >= ConstantHelper.CALL_LEN) {
                            if (ConstantHelper.REFUSE_APPLY_FLAG.equals(originalStatus)) {
                                contactRefuseCallLen++
                            } else if (ConstantHelper.PASS_APPLY_FLAG.equals(originalStatus)) {
                                contactAcceptCallLen++
                            }
                        }
                    }
                }
            }

            if (null != tempMap) {
                //过滤掉二度联系人中的一度联系人
                for (String str : directPhones) {
                    if (tempMap.containsKey(str)) {
                        tempMap.remove(str)
                    }
                }

                //将tempMap改造成map2
                //判断该标签是否包含一度数据
                Set<Map.Entry<String, String>> tempSet = tempMap.entrySet()
                for (Map.Entry<String, String> en : tempSet) {
                    String mark = en.getValue()
                    if (map2.containsKey(mark)) {
                        Integer count = map2.get(mark) + 1
                        map2.put(mark, count)
                    } else {
                        map2.put(mark, 1)
                    }
                }

                //过滤掉二度联系人中的一度联系人
                for (String str : directPhones) {
                    if (tempCallLenMap.containsKey(str)) {
                        tempCallLenMap.remove(str)
                    }
                }

                //将tempMap改造成map2
                //判断该标签是否包含一度数据
                Set<Map.Entry<String, String>> tempCallSet = tempCallLenMap.entrySet()
                for (Map.Entry<String, String> en : tempCallSet) {
                    String mark = en.getValue()
                    if (map2.containsKey(mark)) {
                        Integer count = map2.get(mark) + 1
                        map2.put(mark, count)
                    } else {
                        map2.put(mark, 1)
                    }
                }

                if (tempSet != null) {
                    tempSet.clear()
                    tempSet = null
                }
            }
        }

        memberDeviceAndApplyAndOrderBean.setContactBlackMemberNum(contactBlack)
        memberDeviceAndApplyAndOrderBean.setContactOverdueMemberNum(contactOverdue)
        memberDeviceAndApplyAndOrderBean.setContactAcceptMemberNum(contactAccept)
        memberDeviceAndApplyAndOrderBean.setContactRefuseMemberNum(contactRefuse)

        memberDeviceAndApplyAndOrderBean.setContactBlackMemberCallLenNum(contactBlackCallLen)
        memberDeviceAndApplyAndOrderBean.setContactOverdueMemberCallLenNum(contactOverdueCallLen)
        memberDeviceAndApplyAndOrderBean.setContactAcceptMemberCallLenNum(contactAcceptCallLen)
        memberDeviceAndApplyAndOrderBean.setContactRefuseMemberCallLenNum(contactRefuseCallLen)

        if (tempMap != null) {
            tempMap.clear()
            tempMap = null
        }
        if (directPhones != null) {
            directPhones.clear()
            directPhones = null
        }
        if (ocrs != null) {
            ocrs.clear()
            ocrs = null
        }
    }


    private void insertPhonetagIndex(List<IndexData> indexDatas) {
        if (null != indexDatas) {
                def sql = "insert into phonetag_index (member_id, apply_no, order_no,mobile,index_name,direct,indirect,create_time) " +
                        "values(?,?,?,?,?,?,?,now())"

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
                        ps.setLong(7, indexData.getIndirect())
                    }
                })
        }
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

    private void addIndexDatas(List<IndexData> indexDatas, long memberId, String mobile, String applyNo, String orderNo, String indexName,
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

    private void addIndexMemberDatas(List<IndexData> indexDatas, long memberId, String mobile, String applyNo, String orderNo, String indexName,
                                            long direct,String applyStatus,String orderStatus) {
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

    /**
     * 校验phone合法性
     *
     * @param phone
     * @return
     */
    private static Boolean checkPhone(String phone) {
        if (StringUtils.isBlank(phone)) {
            return false
        }

        if (phone.length() < ConstantHelper.BUSINESS_PHONE_LENGTH) {
            return false
        }

        if (phone.length() >= 2) {
            if (ConstantHelper.BUSINESS_PHONE_1.equals(phone.substring(0, 1))) {
                return false
            }
            if (ConstantHelper.BUSINESS_PHONE_2.equals(phone.substring(0, 1))) {
                return false
            }
        }

        if (phone.length() >= 3) {
            if (ConstantHelper.BUSINESS_PHONE_3.equals(phone.substring(0, 2))) {
                return false
            }
            if (ConstantHelper.BUSINESS_PHONE_4.equals(phone.substring(0, 2))) {
                return false
            }
        }

        if (phone.length() >= 5) {
            if (ConstantHelper.BUSINESS_PHONE_5.equals(phone.substring(0, 4))) {
                return false
            }
            if (ConstantHelper.BUSINESS_PHONE_6.equals(phone.substring(0, 4))) {
                return false
            }
            if (ConstantHelper.BUSINESS_PHONE_7.equals(phone.substring(0, 4))) {
                return false
            }
        }

        if (ConstantHelper.BUSINESS_PHONE_8.equals(phone)) {
            return false
        }

        return true
    }

    protected static String getRid(Object obj) {
        if (obj == null) {
            return null
        }
        if (obj instanceof OResultSet) {
            OResultSet ors = (OResultSet) obj
            if (ors != null && !ors.isEmpty()) {
                return ((ODocument) ors.get(0)).getIdentity().toString()
            }
        } else if (obj instanceof ODocument) {
            return ((ODocument) obj).getIdentity().toString()
        }
        return null
    }
}
