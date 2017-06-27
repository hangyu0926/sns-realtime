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
 * Created by hangyu on 2017/6/14.
 */
@Service
class ApplyRidMemberRidCache {
    private static final Logger LOG = LoggerFactory.getLogger(ApplyRidMemberRidCache.class)

    @Resource
    private OrientSql orientSql

    @Value("#{snsOrientSqlProp.getMemberRidWithApplyRidSql}")
    private String getMemberRidWithApplyRidSql

    @Cacheable(value = 'applyRidMemberRidCache')
    CacheEntry get(appRid) {
        List<ODocument> result = orientSql.execute(getMemberRidWithApplyRidSql + appRid)
        if (CollectionUtils.isEmpty(result)) {
            return null
        }

        String rid = OrientSqlUtil.getRid(result)
        if (StringUtils.isBlank(rid)) {
            return null
        }
        return new CacheEntry(appRid, rid)
    }

    @CachePut(value = 'applyRidMemberRidCache', key = '#cacheEntry.key')
    CacheEntry put(CacheEntry cacheEntry) {
        return cacheEntry
    }

    @Scheduled(cron = '0 0 0 * * ?')
    @CacheEvict(value = "applyRidMemberRidCache", allEntries = true, beforeInvocation = true)
    void clear() {
        LOG.debug("clear cache : applyRidMemberRidCache")
    }
}
