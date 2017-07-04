package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

import cn.memedai.orientdb.sns.realtime.cache.CacheEntry
import cn.memedai.orientdb.sns.realtime.cache.IdCardCache
import cn.memedai.orientdb.sns.realtime.cache.MemberCache
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import cn.memedai.orientdb.sns.realtime.util.OrientSqlUtil
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service

import javax.annotation.Resource

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class CrmMemberToOrientDBServiceImpl implements RealTimeService {

    private static final LOG = LoggerFactory.getLogger(CrmMemberToOrientDBServiceImpl.class)

    @Resource
    private OrientSql orientSql

    @Resource
    private MemberCache memberCache

    @Resource
    private IdCardCache idCardCache

    @Value("#{snsOrientSqlProp.updateMemberWithPhoneSql}")
    private String updateMemberSql

    void process(List<Map<String, Object>> dataList) {
        if (dataList == null) {
            return
        }

        int size = dataList.size()
        for (def i = 0; i < size; i++) {
            Map<String, Object> memberMap = dataList.get(i)
            String memberRid = OrientSqlUtil.getRid(orientSql.execute(updateMemberSql, memberMap.MEMBER_ID,
                    memberMap.MOBILE_NO, memberMap.MEMBER_ID))
            if (StringUtils.isNotBlank(memberRid)) {
                CacheEntry cacheEntry =  memberCache.get(memberMap.MEMBER_ID)
                if (null == cacheEntry ){
                    memberCache.put(new CacheEntry(memberMap.MEMBER_ID, memberRid))
                }
            }
        }
    }

}
