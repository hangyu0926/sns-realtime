package cn.memedai.orientdb.sns.realtime.cache

import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import cn.memedai.orientdb.sns.realtime.util.OrientSqlUtil
import com.orientechnologies.orient.core.record.impl.ODocument
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang.StringUtils
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
class PhoneCache {

    private static final Logger LOG = LoggerFactory.getLogger(PhoneCache.class)

    @Resource
    private OrientSql orientSql

    private String getPhoneSql = 'select from Phone where phone=?'
    private String updatePhoneSql = 'update Phone set phone=? upsert return after where phone=?'

    @Cacheable(value = 'phoneCache')
    CacheEntry get(phone) {
        List<ODocument> result = orientSql.execute(getPhoneSql, phone)
        if (CollectionUtils.isEmpty(result)) {
            String rid = OrientSqlUtil.getRid(orientSql.execute(updatePhoneSql, phone, phone))
            if (StringUtils.isBlank(rid)) {
                return null
            }
            return new CacheEntry(phone, rid)
        }

        String ridOther = OrientSqlUtil.getRid(result)
        if (StringUtils.isBlank(ridOther)) {
            return null
        }
        new CacheEntry(phone, ridOther)
    }


    @CachePut(value = 'phoneCache', key = '#cacheEntry.key')
    CacheEntry put(CacheEntry cacheEntry) {
        return cacheEntry
    }

    @Scheduled(cron = '0 0 0 * * ?')
    @CacheEvict(value = "phoneCache", allEntries = true, beforeInvocation = true)
    void clear() {
        LOG.debug("clear cache : phoneCache")
    }

}
