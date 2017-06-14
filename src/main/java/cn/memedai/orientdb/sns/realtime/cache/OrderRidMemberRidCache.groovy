package cn.memedai.orientdb.sns.realtime.cache

import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import cn.memedai.orientdb.sns.realtime.util.OrientSqlUtil
import com.orientechnologies.orient.core.record.impl.ODocument
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
 * Created by hangyu on 2017/6/14.
 */
@Service
class OrderRidMemberRidCache {
    private static final Logger LOG = LoggerFactory.getLogger(OrderRidMemberRidCache.class)

    @Resource
    private OrientSql orientSql

    private String getPhoneRidSql = 'select expand(in("MemberHasOrder")) from '

    @Cacheable(value = 'orderRidMemberRidCache')
    CacheEntry get(appRid) {
        List<ODocument> result = orientSql.execute(getPhoneRidSql+ appRid)
        if (!StringUtils.isBlank(OrientSqlUtil.getRid(result))) {
            String rid = OrientSqlUtil.getRid(result);
            if (StringUtils.isBlank(rid)) {
                return null
            }
            return new CacheEntry(appRid, rid)
        }
        return null
    }

    @CachePut(value = 'orderRidMemberRidCache', key = '#cacheEntry.key')
    CacheEntry put(CacheEntry cacheEntry) {
        return cacheEntry
    }

    @Scheduled(cron = '0 0 0 * * ?')
    @CacheEvict(value = "orderRidMemberRidCache", allEntries = true, beforeInvocation = true)
    void clear() {
        LOG.debug("clear cache : orderRidMemberRidCache")
    }
}
