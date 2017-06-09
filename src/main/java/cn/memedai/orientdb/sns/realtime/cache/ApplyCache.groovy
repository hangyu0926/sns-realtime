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
class ApplyCache {

    private static final Logger LOG = LoggerFactory.getLogger(ApplyCache.class)

    @Resource
    private OrientSql orientSql

    private String getApplySql = 'select from Apply where applyNo=?'
    private String updateApplySql = 'update Apply set applyNo=? upsert return after where applyNo=?'

    @Cacheable(value = 'applyCache')
    CacheEntry get(applyNo) {
        List<ODocument> result = orientSql.execute(getApplySql, applyNo)
        if (CollectionUtils.isEmpty(result)) {
            return OrientSqlUtil.getRid(orientSql.execute(updateApplySql, applyNo, applyNo))
        }
        new CacheEntry(applyNo, OrientSqlUtil.getRid(result))
    }

    @CachePut(value = 'applyCache', key = '#cacheEntry.key')
    CacheEntry put(CacheEntry cacheEntry) {
        return cacheEntry
    }

    @Scheduled(cron = '0 0 0 * * ?')
    @CacheEvict(value = "applyCache", allEntries = true, beforeInvocation = true)
    void clear() {
        LOG.debug("clear cache : applyCache")
    }

}
