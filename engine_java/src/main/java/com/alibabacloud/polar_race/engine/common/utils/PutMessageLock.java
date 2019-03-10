package com.alibabacloud.polar_race.engine.common.utils;

/**
 * Created by IntelliJ IDEA.
 * User: wenchao.qi
 * Date: 2018/10/28
 * Time: 下午5:39
 */
public interface PutMessageLock {
    void lock();

    void unlock();
}
