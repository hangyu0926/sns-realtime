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
class OrderCache {

    private static final Logger LOG = LoggerFactory.getLogger(OrderCache.class)

    @Resource
    private OrientSql orientSql

    private String getOrderSql = 'select from Order where orderNo=?'
    private String updateOrderSql = 'update Order set orderNo=? upsert return after where orderNo=?'

    @Cacheable(value = 'orderCache')
    CacheEntry get(orderNo) {
        List<ODocument> result = orientSql.execute(getOrderSql, orderNo)
        String orderRid = null
        if (CollectionUtils.isEmpty(result)) {
            orderRid = OrientSqlUtil.getRid(orientSql.execute(updateOrderSql, orderNo, orderNo))
        } else {
            orderRid = OrientSqlUtil.getRid(result)
        }
        new CacheEntry(orderNo, orderRid)
    }

    @CachePut(value = 'orderCache', key = '#cacheEntry.key')
    CacheEntry put(CacheEntry cacheEntry) {
        return cacheEntry
    }

    @Scheduled(cron = '0 0 0 * * ?')
    @CacheEvict(value = "orderCache", allEntries = true, beforeInvocation = true)
    void clear() {
        LOG.debug("clear cache : orderCache")
    }

}
