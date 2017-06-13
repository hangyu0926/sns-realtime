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
class PhoneSourceCache {
    private static final Logger LOG = LoggerFactory.getLogger(PhoneSourceCache.class)

    @Resource
    private OrientSql orientSql

    private String getPhoneSourceSql = 'select from PhoneSource where source=?'
    private String updatePhoneSourceSql = 'update PhoneSource set source=? upsert return after where source=?'

    @Cacheable(value = 'phoneSourceCache')
    CacheEntry get(source) {
        List<ODocument> result = orientSql.execute(getPhoneSourceSql, phone)
        if (CollectionUtils.isEmpty(result)) {
            return OrientSqlUtil.getRid(orientSql.execute(updatePhoneSourceSql, phone, phone))
        }
        new CacheEntry(source, OrientSqlUtil.getRid(result))
    }


    @CachePut(value = 'phoneSourceCache', key = '#cacheEntry.key')
    CacheEntry put(CacheEntry cacheEntry) {
        return cacheEntry
    }

    @Scheduled(cron = '0 0 0 * * ?')
    @CacheEvict(value = "phoneSourceCache", allEntries = true, beforeInvocation = true)
    void clear() {
        LOG.debug("clear cache : phoneSourceCache")
    }
}
