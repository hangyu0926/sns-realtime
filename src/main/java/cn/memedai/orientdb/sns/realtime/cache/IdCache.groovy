package cn.memedai.orientdb.sns.realtime.cache

import org.springframework.cache.annotation.Cacheable
import org.springframework.jdbc.core.JdbcTemplate
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
    private JdbcTemplate jdbcTemplate

    private String idSql = 'select ID_PREFIX,PROVINCE,CITY from credit_audit.ca_sys_value_id_area'

    @Cacheable(value = 'idCache')
    CacheEntry get(idPrefix) {
        new CacheEntry(idPrefix, idMap[idPrefix])
    }


    @PostConstruct
    private void getIdCardMap() {
        jdbcTemplate.queryForList(idSql).each {
            row ->
                idMap[row.ID_PREFIX] = ['ID_PREFIX': row.ID_PREFIX, 'PROVINCE': row.ID_PREFIX, 'CITY': row.CITY]
        }
    }

}
