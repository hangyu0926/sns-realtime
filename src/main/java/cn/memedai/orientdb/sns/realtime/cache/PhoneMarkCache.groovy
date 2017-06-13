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
class PhoneMarkCache {
    private static final Logger LOG = LoggerFactory.getLogger(PhoneMarkCache.class)

    @Resource
    private OrientSql orientSql

    private String getPhoneMarkSql = 'select from PhoneMark where mark=?'
    private String updatePhoneMarkSql = 'update PhoneMark set mark=? upsert return after where mark=?'

    @Cacheable(value = 'phoneMarkCache')
    CacheEntry get(mark) {
        List<ODocument> result = orientSql.execute(getPhoneMarkSql, phone)
        if (CollectionUtils.isEmpty(result)) {
            return OrientSqlUtil.getRid(orientSql.execute(updatePhoneMarkSql, phone, phone))
        }
        new CacheEntry(mark, OrientSqlUtil.getRid(result))
    }


    @CachePut(value = 'phoneMarkCache', key = '#cacheEntry.key')
    CacheEntry put(CacheEntry cacheEntry) {
        return cacheEntry
    }

    @Scheduled(cron = '0 0 0 * * ?')
    @CacheEvict(value = "phoneMarkCache", allEntries = true, beforeInvocation = true)
    void clear() {
        LOG.debug("clear cache : phoneMarkCache")
    }
}
