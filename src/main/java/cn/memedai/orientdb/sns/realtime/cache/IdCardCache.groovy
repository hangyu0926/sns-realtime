package cn.memedai.orientdb.sns.realtime.cache

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.cache.annotation.CacheEvict
import org.springframework.cache.annotation.CachePut
import org.springframework.cache.annotation.Cacheable
import org.springframework.stereotype.Service

/**
 * Created by hangyu on 2017/6/14.
 */
@Service
class IdCardCache {
    private static final Logger LOG = LoggerFactory.getLogger(IdCardCache.class)

    @Cacheable(value = 'idCardCache')
    CacheEntry get(memberId) {
        return null
    }

    @CachePut(value = 'idCardCache', key = '#cacheEntry.key')
    CacheEntry put(CacheEntry cacheEntry) {
        return cacheEntry
    }

    @CacheEvict(value = "idCardCache", allEntries = true, beforeInvocation = true)
    void clear() {
        LOG.debug("clear cache : idCardCache")
    }
}
