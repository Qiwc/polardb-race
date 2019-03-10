package com.alibabacloud.polar_race.engine.common.utils;

/**
 * Created by IntelliJ IDEA.
 * User: wenchao.qi
 * Date: 2018/10/16
 * Time: 下午4:12
 */
public class ByteToLong {
    public static long byteArrayToLong(byte[] b) {

        return ((((long) b[7]) << 56) |
                (((long) b[6] & 0xff) << 48) |
                (((long) b[5] & 0xff) << 40) |
                (((long) b[4] & 0xff) << 32) |
                (((long) b[3] & 0xff) << 24) |
                (((long) b[2] & 0xff) << 16) |
                (((long) b[1] & 0xff) << 8) |
                (((long) b[0] & 0xff)));
    }

    public static long byteArrayToLong_seven(byte[] b) {

        return (
                (((long) b[1] & 0xff) << 48) |
                        (((long) b[2] & 0xff) << 40) |
                        (((long) b[3] & 0xff) << 32) |
                        (((long) b[4] & 0xff) << 24) |
                        (((long) b[5] & 0xff) << 16) |
                        (((long) b[6] & 0xff) << 8) |
                        (((long) b[7] & 0xff)));
    }

}
