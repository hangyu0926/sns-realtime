package cn.memedai.orientdb.sns.realtime.cache

import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import cn.memedai.orientdb.sns.realtime.util.OrientSqlUtil
import com.orientechnologies.orient.core.record.impl.ODocument
import org.apache.commons.collections.CollectionUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.cache.annotation.CacheEvict
import org.springframework.cache.annotation.CachePut
import org.springframework.cache.annotation.Cacheable
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service

import javax.annotation.Resource

/**
 * Created by kisho on 2017/6/9.
 */
@Service
class MemberCache {

    private static final Logger LOG = LoggerFactory.getLogger(MemberCache.class)

    @Resource
    private OrientSql orientSql

    private String getMemberSql = 'select from Member where memberId=?'
    private String updateMemberSql = 'update Member set memberId=? upsert return after where memberId=?'

    @Cacheable(value = 'memberCache')
    CacheEntry get(memberId, insertIfNotExist) {
        List<ODocument> result = orientSql.execute(getMemberSql, memberId)
        if (CollectionUtils.isEmpty(result) && insertIfNotExist) {
            return OrientSqlUtil.getRid(orientSql.execute(updateMemberSql, memberId, memberId))
        }
        new CacheEntry(memberId, OrientSqlUtil.getRid(result))
    }

    @Cacheable(value = 'memberCache')
    CacheEntry get(memberId) {
        get(memberId, true)
    }

    @CachePut(value = 'memberCache', key = '#cacheEntry.key')
    CacheEntry put(CacheEntry cacheEntry) {
        return cacheEntry
    }

    @Scheduled(cron = '0 0 0 * * ?')
    @CacheEvict(value = "memberCache", allEntries = true, beforeInvocation = true)
    void clear() {
        LOG.debug("clear cache : memberCache")
    }

}
