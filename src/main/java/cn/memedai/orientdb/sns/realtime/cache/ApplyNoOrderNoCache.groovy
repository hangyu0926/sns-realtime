package cn.memedai.orientdb.sns.realtime.cache

import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import cn.memedai.orientdb.sns.realtime.util.OrientSqlUtil
import com.orientechnologies.orient.core.record.impl.ODocument
import com.orientechnologies.orient.core.sql.query.OBasicResultSet
import groovy.sql.Sql
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
 * Created by hangyu on 2017/6/15.
 */
@Service
class ApplyNoOrderNoCache {
    private static final Logger LOG = LoggerFactory.getLogger(ApplyNoOrderNoCache.class)

    @Resource
    private OrientSql orientSql

    @Resource
    private Sql sql

    @Value("#{snsOrientSqlProp.selectOrderFromApplySql}")
    private selectOrderFromApplySql

    @Value("#{sqlProp.selectOrderFromApplyMysql}")
    private selectOrderFromApplyMysql

    @Cacheable(value = 'applyNoOrderNoCache')
    CacheEntry get(appNo) {
        //取不到查orientDb，然后查mysql
        List<ODocument> orderNoResult = orientSql.execute(selectOrderFromApplySql,appNo)
        String orderNo =  null
        if (null != orderNoResult && !CollectionUtils.isEmpty(orderNoResult)) {
            ODocument orderNoDocument = orderNoResult.get(0)
            orderNo = orderNoDocument.field("orderNo") != null ? orderNoDocument.field("orderNo").toString() : null
            if (null == orderNo){
                orderNo = sql.firstRow(selectFromApplyMysql,[appNo]).orderNo
            }
            return new CacheEntry(appNo, orderNo)
        }else{
            orderNo = sql.firstRow(selectFromApplyMysql,[appNo]).orderNo
            return new CacheEntry(appNo, orderNo)
        }
    }

    @CachePut(value = 'applyNoOrderNoCache', key = '#cacheEntry.key')
    CacheEntry put(CacheEntry cacheEntry) {
        return cacheEntry
    }

    @Scheduled(cron = '0 0 0 * * ?')
    @CacheEvict(value = "applyNoOrderNoCache", allEntries = true, beforeInvocation = true)
    void clear() {
        LOG.debug("clear cache : applyNoOrderNoCache")
    }
}
