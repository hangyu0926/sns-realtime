package cn.memedai.orientdb.sns.realtime.service
/**
 * Created by kisho on 2017/6/9.
 */
interface RealTimeService {

    void process(List<Map<String, Object>> dataList)

}
