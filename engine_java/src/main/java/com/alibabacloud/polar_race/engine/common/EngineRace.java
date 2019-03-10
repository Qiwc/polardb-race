package com.alibabacloud.polar_race.engine.common;

import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.impl.DBImpl;

public class EngineRace extends AbstractEngine {

    DBImpl db;

    @Override
    public void open(String path) throws EngineException {
//		System.out.println("=======================db open=========================");
//		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
//		System.out.println(df.format(new Date()));// new Date()为获取当前系统时间
        db = new DBImpl(path);
    }

    @Override
    public void write(byte[] key, byte[] value) throws EngineException {
        db.write(key, value);
    }

    @Override
    public byte[] read(byte[] key) throws EngineException {
        return db.read(key);
    }

    @Override
    public void range(byte[] lower, byte[] upper, AbstractVisitor visitor) throws EngineException {
    }


    @Override
    public void close() {
//	    System.out.println("=======================db close======================");
//		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
//		System.out.println(df.format(new Date()));// new Date()为获取当前系统时间
        db.close();
        db = null;
        System.gc();
    }

}
