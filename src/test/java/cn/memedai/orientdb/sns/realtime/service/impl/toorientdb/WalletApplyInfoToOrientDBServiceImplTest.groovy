package cn.memedai.orientdb.sns.realtime.service.impl.toorientdb

import cn.memedai.orientdb.sns.realtime.AbstractRealTimeTest
import org.junit.Test

/**
 * Created by kisho on 2017/6/9.
 */
class WalletApplyInfoToOrientDBServiceImplTest extends AbstractRealTimeTest {

    @Test
    void testProcess() {
        produce('com.mime.bdp.dts.wallet', 'apply_info', [[
                                                 'cellphone'       : '15821180279',
                                                 'apply_no'        : '1485313547297000',
                                                 'member_id'       : 715157L,
                                                 'created_datetime': '2017-06-13 00:00:00',
                                                 'apply_status'    : 4000,
                                                 'store_id'        : '2759',
                                                 'order_no'        : '1496921804405003'
                                         ]])
    }

}
