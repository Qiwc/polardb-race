package com.alibabacloud.polar_race.engine.common.utils;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: chenyifan
 * Date: 2018-07-11
 * Time: 下午2:19
 */
public class UnsafeUtil {

    public static Unsafe UNSAFE;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

