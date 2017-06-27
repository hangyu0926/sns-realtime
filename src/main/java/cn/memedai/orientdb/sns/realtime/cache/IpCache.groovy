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
class IpCache {
    private static final Logger LOG = LoggerFactory.getLogger(IpCache.class)

    @Resource
    private OrientSql orientSql

    @Value("#{snsOrientSqlProp.getIpSql}")
    private String getIpSql

    @Value("#{snsOrientSqlProp.updateIpSql}")
    private String updateIpSql

    @Cacheable(value = 'ipCache')
    CacheEntry get(ip) {
        String ipAndipCity = ip
        String tempIp = ipAndipCity.split("\\|")[0]
        String tempIpCity =ipAndipCity.split("\\|")[1]

        List<ODocument> result = orientSql.execute(getIpSql, tempIp)
        if (CollectionUtils.isEmpty(result)) {
            String rid = OrientSqlUtil.getRid(orientSql.execute(updateIpSql, tempIp,tempIpCity, tempIp))
            if (StringUtils.isBlank(rid)) {
                return null
            }
            return new CacheEntry(ipAndipCity, rid)
        }
        String ridOther = OrientSqlUtil.getRid(result)
        if (StringUtils.isBlank(ridOther)) {
            return null
        }
        new CacheEntry(ipAndipCity, ridOther)
    }

    @CachePut(value = 'ipCache', key = '#cacheEntry.key')
    CacheEntry put(CacheEntry cacheEntry) {
        return cacheEntry
    }

    @Scheduled(cron = '0 0 0 * * ?')
    @CacheEvict(value = "ipCache", allEntries = true, beforeInvocation = true)
    void clear() {
        LOG.debug("clear cache : ipCache")
    }
}
