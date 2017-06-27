package cn.memedai.orientdb.sns.realtime.cache

import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import com.orientechnologies.orient.core.record.impl.ODocument
import groovy.sql.Sql
import org.apache.commons.collections.CollectionUtils
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
 * Created by hangyu on 2017/6/27.
 */
@Service
class ApplyNoPhoneCache {
    private static final Logger LOG = LoggerFactory.getLogger(ApplyNoPhoneCache.class)

    @Resource
    private OrientSql orientSql

    @Resource
    private Sql sql

    @Value("#{snsOrientSqlProp.selectPhoneFromApplySql}")
    private selectPhoneFromApplySql

    @Value("#{sqlProp.selectPhoneFromApplyMySql}")
    private selectPhoneFromApplyMySql

    @Cacheable(value = 'applyNoPhoneCache')
    CacheEntry get(appNo) {
        //取不到查orientDb，然后查mysql
        List<ODocument> phoneResult = orientSql.execute(selectPhoneFromApplySql,appNo)
        String phone =  null
        if (null != phoneResult && !CollectionUtils.isEmpty(phoneResult)) {
            ODocument orderNoDocument = phoneResult.get(0)
            phone = orderNoDocument.field("phone") != null ? orderNoDocument.field("phone").toString() : null
            if (null == phone){
                phone = sql.firstRow(selectPhoneFromApplyMySql,[appNo]).phone
            }
            return new CacheEntry(appNo, phone)
        }else{
            phone = sql.firstRow(selectPhoneFromApplyMySql,[appNo]).phone
            return new CacheEntry(appNo, phone)
        }
    }

    @CachePut(value = 'applyNoPhoneCache', key = '#cacheEntry.key')
    CacheEntry put(CacheEntry cacheEntry) {
        return cacheEntry
    }

    @Scheduled(cron = '0 0 0 * * ?')
    @CacheEvict(value = "applyNoPhoneCache", allEntries = true, beforeInvocation = true)
    void clear() {
        LOG.debug("clear cache : applyNoPhoneCache")
    }
}
