package cn.memedai.orientdb.sns.realtime.cache

import groovy.sql.Sql
import org.springframework.beans.factory.annotation.Value
import org.springframework.cache.annotation.Cacheable
import org.springframework.stereotype.Service

import javax.annotation.PostConstruct
import javax.annotation.Resource

/**
 * Created by kisho on 2017/6/9.
 */
@Service
class IdCache {

    private Map<String, Map<String, String>> idMap = [:]

    @Resource
    private Sql sql

    @Value("#{sqlProp.selectIdAreaMysql}")
    private String idSql

    @Cacheable(value = 'idCache')
    CacheEntry get(idPrefix) {
        new CacheEntry(idPrefix, idMap[idPrefix])
    }


    @PostConstruct
    private void getIdCardMap() {
        sql.eachRow(idSql) {
            row ->
                idMap[row.ID_PREFIX] = ['ID_PREFIX': row.ID_PREFIX, 'PROVINCE': row.ID_PREFIX, 'CITY': row.CITY]
        }
    }

}
