package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

import cn.memedai.orientdb.sns.realtime.cache.CacheEntry
import cn.memedai.orientdb.sns.realtime.cache.IdCardCache
import cn.memedai.orientdb.sns.realtime.cache.MemberCache
import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

import javax.annotation.Resource

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class IdCardToOrientDBServiceImpl implements RealTimeService {

    private static final LOG = LoggerFactory.getLogger(IdCardToOrientDBServiceImpl.class)

    @Resource
    private OrientSql orientDb

    @Resource
    private IdCardCache idCardCache

    void process(List<Map<String, Object>> dataList) {
        if (dataList == null) {
            return
        }

        int size = dataList.size()
        for (def i = 0; i < size; i++) {
            Map<String, Object> idCardMap = dataList.get(i)
            if (idCardMap != null) {
                idCardCache.put(new CacheEntry(idCardMap.ID_PREFIX, idCardMap))
            }
        }
    }

}
