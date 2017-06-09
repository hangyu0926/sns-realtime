package cn.memedai.orientdb.sns.realtime.cache

/**
 * Created by kisho on 2017/6/9.
 */
class CacheEntry {
    def key
    def value

    CacheEntry(key, value) {
        this.key = key
        this.value = value
    }
}
