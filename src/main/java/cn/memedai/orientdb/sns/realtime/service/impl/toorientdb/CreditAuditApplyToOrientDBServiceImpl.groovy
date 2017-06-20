package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

import cn.memedai.orientdb.sns.realtime.cache.ApplyCache
import cn.memedai.orientdb.sns.realtime.cache.CacheEntry
import cn.memedai.orientdb.sns.realtime.service.RealTimeService
import cn.memedai.orientdb.sns.realtime.sql.OrientSql
import cn.memedai.orientdb.sns.realtime.util.OrientSqlUtil
import org.apache.commons.lang.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

import javax.annotation.Resource

/**
 * Created by hangyu on 2017/6/19.
 */
@Service
class CreditAuditApplyToOrientDBServiceImpl implements RealTimeService{
    private static final LOG = LoggerFactory.getLogger(CreditAuditApplyToOrientDBServiceImpl.class)

    @Resource
    private OrientSql orientSql

    @Resource
    private ApplyCache applyCache

    private String updateApplySql = 'update Apply set applyNo=?,status=? upsert return after where applyNo=?'

    void process(List<Map<String, Object>> dataList) {
        if (dataList == null) {
            return
        }

        if (dataList.size() == 0) {
            return
        }
        Map<String, Object> applyMap = dataList.get(0)

        String applyRid = OrientSqlUtil.getRid(orientSql.execute(updateApplySql, applyMap.APPL_NO, applyMap.PASS, applyMap.APPL_NO))
        if (StringUtils.isNotBlank(applyRid)) {
            applyCache.put(new CacheEntry(applyMap.apply_no, applyRid))
        }
    }
}
