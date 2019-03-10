package benchmark;

import java.util.Random;
import java.util.concurrent.Callable;

import benchmark.util.BenchMarkUtil;
import com.alibabacloud.polar_race.engine.common.AbstractEngine;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.alibabacloud.polar_race.engine.common.exceptions.RetCodeEnum;

public class EngineReader implements Callable<Integer>{
	public int readCount;
	public AbstractEngine engine;
	public int keyLen = 4;
	
	public int keyRange = 0;
	
	public int readSuccessCount = 0;
	public long orginSeed = 0;
	
	public EngineReader(int readCount, AbstractEngine engine, int keyLen, int keyRange, long seed) {
		this.readCount = readCount;
		this.engine = engine;
		this.keyLen = keyLen;
		this.keyRange = keyRange;
		this.orginSeed = seed;
	}

	public Integer call() throws Exception {
		byte[] key = null;
		byte[] value = null;
		Random random = new Random(this.orginSeed);
		for (int i = 0; i < this.readCount; i++) {
			key = BenchMarkUtil.getRandomKey(keyLen, random);
			try {
				value = this.engine.read(key);
				readSuccessCount++;
			} catch (EngineException e) {
				if(e.retCode.equals(RetCodeEnum.NOT_FOUND)) {
					continue;
				}else {
					e.printStackTrace();
					throw e;
				}
			}
		}
		return readSuccessCount;
	}

}
