package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

import cn.memedai.orientdb.sns.realtime.cache.CacheEntry
import cn.memedai.orientdb.sns.realtime.cache.IdCardCache
import cn.memedai.orientdb.sns.realtime.cache.MemberCache
import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import cn.memedai.orientdb.sns.realtime.util.OrientSqlUtil
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

import javax.annotation.Resource

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class MemberToOrientDBServiceImpl implements RealTimeService {

    private static final LOG = LoggerFactory.getLogger(MemberToOrientDBServiceImpl.class)

    @Resource
    private OrientSql orientSql

    @Resource
    private MemberCache memberCache

    @Resource
    private IdCardCache idCardCache

    private String updateMemberSql = 'update Member set memberId=?,name=?,idNo=?,province=?,city=?,phone=?,isBlack=false,isOverdue=false upsert return after where memberId=?'

    void process(List<Map<String, Object>> dataList) {
        if (dataList == null) {
            return
        }

        int size = dataList.size()
        for (def i = 0; i < size; i++) {
            Map<String, Object> memberMap = dataList.get(i)

            String idNo = memberMap.ID_NO
            String province = memberMap.PROVINCE
            String city = memberMap.CITY

            if (idNo != null && idNo.trim().length() > 6) {
                Map<String, String> idAddress = null
                CacheEntry idCardCacheEntry = idCardCache.get(idNo.substring(0, 6))
                if (idCardCacheEntry != null) {
                    idAddress = idCardCacheEntry.value
                }
                if (idAddress != null) {
                    province = idAddress.PROVINCE
                    city = idAddress.city
                }
            }

            String memberRid = OrientSqlUtil.getRid(orientSql.execute(updateMemberSql, memberMap.MEMBER_ID, memberMap.NAME, idNo, province, city,
                    memberMap.MOBILE_NO, memberMap.MEMBER_ID))
            if (StringUtils.isNotBlank(memberRid)) {
                memberCache.put(new CacheEntry(memberMap.MEMBER_ID, memberRid))
            }
        }
    }

}
