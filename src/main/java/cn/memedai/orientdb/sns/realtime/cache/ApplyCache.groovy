package cn.memedai.orientdb.sns.realtime.cache

import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import cn.memedai.orientdb.sns.realtime.util.OrientSqlUtil
import com.orientechnologies.orient.core.record.impl.ODocument
import org.apache.commons.collections.CollectionUtils
import org.apache.commons.lang.StringUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
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

    @Value("#{snsOrientSqlProp.getApplySql}")
    private String getApplySql

    @Value("#{snsOrientSqlProp.updateApplyNoSql}")
    private String updateApplyNoSql

    @Cacheable(value = 'applyCache')
    CacheEntry get(applyNo) {
        List<ODocument> result = orientSql.execute(getApplySql, applyNo)
        if (CollectionUtils.isEmpty(result)) {
            String rid = OrientSqlUtil.getRid(orientSql.execute(updateApplyNoSql, applyNo, applyNo))
            if (StringUtils.isBlank(rid)) {
                return null
            }
            return new CacheEntry(applyNo, rid)
        }
        String ridOther = OrientSqlUtil.getRid(result)
        if (StringUtils.isBlank(ridOther)) {
            return null
        }
        new CacheEntry(applyNo, ridOther)
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
