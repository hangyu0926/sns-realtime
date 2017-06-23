package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

import cn.memedai.orientdb.sns.realtime.cache.CacheEntry
import cn.memedai.orientdb.sns.realtime.cache.PhoneCache
import cn.memedai.orientdb.sns.realtime.cache.PhoneMarkCache
import cn.memedai.orientdb.sns.realtime.cache.PhoneSourceCache
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

import javax.annotation.Resource

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class PhoneTagToOrientDBServiceImpl implements RealTimeService {

    private static final LOG = LoggerFactory.getLogger(PhoneTagToOrientDBServiceImpl.class)

    @Resource
    private PhoneCache phoneCache

    @Resource
    private PhoneMarkCache phoneMarkCache

    @Resource
    private PhoneSourceCache phoneSourceCache

    @Resource
    private OrientSql orientSql

    void process(List<Map<String, Object>> dataList) {
        if (dataList == null) {
            return
        }

        if (dataList.size() == 0) {
            return
        }
        Map<String, Object> phoneTagMap = dataList.get(0)

        String phone = (String) phoneTagMap.PHONE_NO

        String phoneRid = null
        CacheEntry phoneCacheEntry = phoneCache.get(phone)
        if (phoneCacheEntry != null) {
            phoneRid = phoneCacheEntry.value
        }

        if (StringUtils.isNotBlank(phoneTagMap.PHONE_TYPE)) {
            String phoneMarkRid = phoneMarkCache.get(phoneTagMap.PHONE_TYPE).value
            if (StringUtils.isNotBlank(phoneMarkRid) && StringUtils.isNotBlank(phoneRid)) {
                orientSql.createEdge('HasPhoneMark', phoneRid, phoneMarkRid)
            }
        }

        if (StringUtils.isNotBlank(phoneTagMap.SOURCE)) {
            String phoneSourceRid = phoneSourceCache.get(phoneTagMap.SOURCE).value
            if (StringUtils.isNotBlank(phoneRid) && StringUtils.isNotBlank(phoneSourceRid)) {
                orientSql.createEdge('HasPhoneSource', phoneRid, phoneSourceRid)
            }
        }

    }

}
