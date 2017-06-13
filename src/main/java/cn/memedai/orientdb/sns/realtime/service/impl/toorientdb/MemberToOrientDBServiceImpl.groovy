package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

import cn.memedai.orientdb.sns.realtime.cache.CacheEntry
import cn.memedai.orientdb.sns.realtime.cache.MemberCache
import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import cn.memedai.orientdb.sns.realtime.util.OrientSqlUtil
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

    private String updateMemberSql = 'update Member set memberId=?,name=?,idNo=?,province=?,city=?,phone=?,isBlack=false,isOverdue=false upsert return after where memberId=?'

    void process(List<Map<String, Object>> dataList) {
        for (def i = 0; i < dataList.size(); i++){
            Map<String, Object> memberMap = dataList.get(i)

            String memberRid = OrientSqlUtil.getRid(orientSql.execute(updateMemberSql, memberMap.MEMBER_ID,memberMap.NAME,memberMap.ID_NO,memberMap.PROVINCE,memberMap.CITY,
                    memberMap.MOBILE_NO,memberMap.MEMBER_ID))

            memberCache.put(new CacheEntry(memberMap.MEMBER_ID, memberRid))
        }
    }

}
