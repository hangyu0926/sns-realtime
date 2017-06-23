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
 * Created by hangyu on 2017/6/13.
 */
@Service
class ApplyHasDoCache {
    private static final Logger LOG = LoggerFactory.getLogger(ApplyHasDoCache.class)

    @Cacheable(value = 'applyHasDoCache')
    CacheEntry get(applyNo) {
        return null
    }

    @CachePut(value = 'applyHasDoCache', key = '#cacheEntry.key')
    CacheEntry put(CacheEntry cacheEntry) {
        return cacheEntry
    }

    //@Scheduled(cron = '0 0 0 * * ?')
    @CacheEvict(value = "applyHasDoCache", allEntries = true, beforeInvocation = true)
    void clear() {
        LOG.debug("clear cache : applyHasDoCache")
    }
}
