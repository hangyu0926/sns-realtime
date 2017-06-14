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
 * Created by hangyu on 2017/6/14.
 */
@Service
class DeviceCache {
    private static final Logger LOG = LoggerFactory.getLogger(DeviceCache.class)

    @Resource
    private OrientSql orientSql

    private String getDeviceSql = 'select from Device where deviceId=?'
    private String updateDeviceSql = 'update Device set deviceId=? upsert return after where deviceId=?'

    @Cacheable(value = 'deviceCache')
    CacheEntry get(deviceId) {
        List<ODocument> result = orientSql.execute(getDeviceSql, deviceId)
        if (CollectionUtils.isEmpty(result)) {
            String rid = OrientSqlUtil.getRid(orientSql.execute(updateDeviceSql, deviceId, deviceId))
            if (StringUtils.isBlank(rid)) {
                return null
            }
            return new CacheEntry(deviceId, rid)
        }
        String ridOther = OrientSqlUtil.getRid(result)
        if (StringUtils.isBlank(ridOther)) {
            return null
        }
        new CacheEntry(deviceId, ridOther)
    }

    @CachePut(value = 'deviceCache', key = '#cacheEntry.key')
    CacheEntry put(CacheEntry cacheEntry) {
        return cacheEntry
    }

    @Scheduled(cron = '0 0 0 * * ?')
    @CacheEvict(value = "deviceCache", allEntries = true, beforeInvocation = true)
    void clear() {
        LOG.debug("clear cache : deviceCache")
    }
}
