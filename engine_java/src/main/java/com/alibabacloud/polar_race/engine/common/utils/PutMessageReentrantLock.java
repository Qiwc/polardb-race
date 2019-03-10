package com.alibabacloud.polar_race.engine.common.utils;

import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by IntelliJ IDEA.
 * User: wenchao.qi
 * Date: 2018/10/16
 * Time: 上午10:35
 */
public class PutMessageReentrantLock implements PutMessageLock {
    private ReentrantLock putMessageNormalLock = new ReentrantLock(); // NonfairSync

    public void lock() {
        putMessageNormalLock.lock();
    }

    public void unlock() {
        putMessageNormalLock.unlock();
    }
}
