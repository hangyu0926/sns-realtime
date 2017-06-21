package cn.memedai.orientdb.sns.realtime.sql

import com.orientechnologies.common.concur.ONeedRetryException
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx
import com.orientechnologies.orient.core.sql.OCommandSQL
import org.apache.commons.collections.CollectionUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

import javax.annotation.Resource
import java.text.MessageFormat

/**
 * Created by kisho on 2017/6/8.
 */
@Service
class OrientSql {

    private static final Logger LOG = LoggerFactory.getLogger(OrientSql.class)

    private static ODatabaseDocumentTx tx

    private String checkEdgeSql = 'select from (select expand(out_{0}) from {1}) where in = {2}'
    private String createEdgeSql = 'create edge {0} from {1} to {2}'

    @Resource
    private Properties orientDbConfig

    private ODatabaseDocumentTx getTx() {
        if (tx == null) {
            tx = new ODatabaseDocumentTx(orientDbConfig.url)
            tx.open(orientDbConfig.userName, orientDbConfig.password)
        }
        return tx
    }

    public <RET> RET execute(String sql, Object... args) {
        long start = System.currentTimeMillis()

        RET ret = null
        int retry = 0
        while (++retry <= 20) {
            try {
                ret = getTx().command(new OCommandSQL(sql)).execute(args)
                break
            } catch (ONeedRetryException e) {
                try {
                    Thread.sleep(100 * retry)
                } catch (InterruptedException e1) {
                    LOG.error('', e1)
                }
                continue
            } catch (Throwable e) {
                LOG.error("{} @ {}", sql, e.getMessage())
                LOG.error("", e)
                break
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("$sql,$args,${System.currentTimeMillis() - start}ms")
        }
        ret
    }

    void createEdge(String edge, String fromRid, String toRid) {
        if (CollectionUtils.isEmpty(execute(MessageFormat.format(checkEdgeSql, edge, fromRid, toRid)))) {
            execute(MessageFormat.format(createEdgeSql, edge, fromRid, toRid))
        }
    }

}
