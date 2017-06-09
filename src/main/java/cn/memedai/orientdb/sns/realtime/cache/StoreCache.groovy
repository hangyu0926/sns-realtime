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
class StoreCache {

    private static final Logger LOG = LoggerFactory.getLogger(StoreCache.class)

    @Resource
    private OrientSql orientSql

    private String getStoreSql = 'select from Store where storeId=?'
    private String updateStoreSql = 'update Store set storeId=? upsert return after where storeId=?'

    @Cacheable(value = 'storeCache')
    CacheEntry get(storeId) {
        List<ODocument> result = orientSql.execute(getStoreSql, storeId)
        if (CollectionUtils.isEmpty(result)) {
            return OrientSqlUtil.getRid(orientSql.execute(updateStoreSql, storeId, storeId))
        }
        new CacheEntry(storeId, OrientSqlUtil.getRid(result))
    }

    @CachePut(value = 'storeCache', key = '#cacheEntry.key')
    CacheEntry put(CacheEntry cacheEntry) {
        return cacheEntry
    }

    @Scheduled(cron = '0 0 0 * * ?')
    @CacheEvict(value = "storeCache", allEntries = true, beforeInvocation = true)
    void clear() {
        LOG.debug("clear cache : storeCache")
    }

}