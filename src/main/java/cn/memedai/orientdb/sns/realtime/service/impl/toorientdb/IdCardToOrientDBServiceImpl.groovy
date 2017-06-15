package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

import cn.memedai.orientdb.sns.realtime.RealTimeDispatch
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
class IdCardToOrientDBServiceImpl implements RealTimeService {

    private static final LOG = LoggerFactory.getLogger(IdCardToOrientDBServiceImpl.class)

    @Resource
    private OrientSql orientSql

    @Resource
    private MemberCache memberCache

    @Resource
    private IdCardCache idCardCache

    private String updateMemberSql = 'update Member set memberId=?,name=?,idNo=?,province=?,city=? upsert return after where memberId=?'


    void process(List<Map<String, Object>> dataList) {
        if (dataList == null) {
            return
        }

        int size = dataList.size()
        for (def i = 0; i < size; i++) {
            Map<String, Object> idCardMap = dataList.get(i)

            String memberId = idCardMap.MEMBER_ID
            String name = idCardMap.NAME
            String idNo = idCardMap.ID_NO
            String province = idCardMap.PROVINCE
            String city = idCardMap.CITY

            if (idNo != null && idNo.trim().length() > 6) {
                Map<String,String> map = null
                map = RealTimeDispatch.idCardMap.get(idNo.substring(0, 6))
                if (map != null) {
                    province = map.get("PROVINCE")
                    city = map.get("CITY")
                }
            }

            String memberRid = OrientSqlUtil.getRid(orientSql.execute(updateMemberSql,memberId,
                    name,idNo, province, city,memberId))
            if (StringUtils.isNotBlank(memberRid)) {
                CacheEntry cacheEntry =  memberCache.get(memberId)
                if (null == cacheEntry ){
                    memberCache.put(new CacheEntry(memberId, memberRid))
                }
            }
        }
    }

}
