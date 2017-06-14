package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

import cn.memedai.orientdb.sns.realtime.cache.CacheEntry
import cn.memedai.orientdb.sns.realtime.cache.PhoneCache
import cn.memedai.orientdb.sns.realtime.cache.PhoneMarkCache
import cn.memedai.orientdb.sns.realtime.cache.PhoneSourceCache
import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import cn.memedai.orientdb.sns.realtime.util.OrientSqlUtil
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

import javax.annotation.Resource

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class PhoneTagOrientDBServiceImpl implements RealTimeService {

    private static final LOG = LoggerFactory.getLogger(PhoneTagOrientDBServiceImpl.class)

    @Resource
    private PhoneCache phoneCache

    @Resource
    private PhoneMarkCache phoneMarkCache

    @Resource
    private PhoneSourceCache phoneSourceCache

    @Resource
    private OrientSql orientSql

    void process(List<Map<String, Object>> dataList) {
        Map<String, Object> phoneTagMap = dataList.get(0)

        String phone = (String) phoneTagMap.PHONE_NO

        String phoneRid = phoneCache.get(phone).value

        if (StringUtils.isNotBlank(phoneTagMap.PHONE_TYPE)) {
            String phoneMarkRid = phoneMarkCache.get(phoneTagMap.PHONE_TYPE);
            orientSql.createEdge('HasPhoneMark', phoneRid, phoneMarkRid)
        }

        if (StringUtils.isNotBlank(phoneTagMap.SOURCE)) {
            String phoneSourceRid = phoneSourceCache.get(phoneTagMap.SOURCE)
            orientSql.createEdge('HasPhoneSource', phoneRid, phoneSourceRid)
        }

    }

}
