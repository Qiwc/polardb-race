package benchmark;

import java.text.DecimalFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import benchmark.util.BenchMarkUtil;
import com.alibabacloud.polar_race.engine.common.AbstractEngine;
import com.alibabacloud.polar_race.engine.common.EngineRace;
import com.alibabacloud.polar_race.engine.common.exceptions.EngineException;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class BenchMark {
	
	private static Logger logger = LoggerFactory.getLogger(BenchMark.class);
	
	private static void usage() {
		String msg = "java -Xms3072m -Xmx3072m  -XX:MaxDirectMemorySize=64m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:NewRatio=1 -cp ./benchmark_example_java-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.alibabacloud.polar_race.benchmark.BenchMark threadCount writeCount keyLen_byte valueLen_byte db_path";
		System.out.println(msg);
	}
	
	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
//		if(args.length != 5) {
//			usage();
//			System.exit(-1);
//		}
		int threadCount = 64;
		int writeCount  = 100000;
		int keyLen      = 8;
		int valueLen    = 4096;
		String dbPath   = "tmp";
		
		long fillRandomIOPS = 0;
		long readRandomIOPS = 0;
		long readSeqIOPS = 0;
		int operationSum = writeCount * threadCount;
		int readSeqCount = 2;
		
		BenchMarkUtil.clearDBPath(dbPath);
		
		ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(threadCount));
		ScheduledExecutorService executor = MoreExecutors.getExitingScheduledExecutorService((ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1));
		MemChecker memChecker = new MemChecker(0);
		ScheduledFuture<Long> memCheckFuture = (ScheduledFuture<Long>) executor.scheduleAtFixedRate(memChecker, 0, 100, TimeUnit.MILLISECONDS); 
		List<ListenableFuture<Integer>> futures = Lists.newArrayList();
		List<EngineWriter> engineWriters = Lists.newArrayList();
		AbstractEngine engine = new EngineRace();
		long writeSeed = 0;
		for (int i = 0; i < threadCount; i++) 
			engineWriters.add(new EngineWriter(writeCount, engine, valueLen, keyLen, operationSum, writeSeed++));
		
		ListenableFuture<List<Integer>> resultsFuture;
		
		logger.error("Date:			" + new Date(System.currentTimeMillis()));
		logger.error("Keys:			" + keyLen + " bytes each");
		logger.error("Values:		" + valueLen + " bytes each");
		logger.error("Entries:			" + operationSum);
		logger.error("------------------------------------------------------------------------");
		long startTime = System.currentTimeMillis();
		try {
			engine.open(dbPath);
		} catch (EngineException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		long opendbTime = System.currentTimeMillis();
		logger.error("open db success, used: " + (opendbTime - startTime) + "ms");
		
		//random write
		logger.error("Fill random");
		logger.error("Drop caches...");
		BenchMarkUtil.dropPageCache();
        long clearWritePageCacheTime = System.currentTimeMillis();
        logger.error("Drop caches done, used: " + (clearWritePageCacheTime - opendbTime) + "ms");
        logger.error("DB path:[" + dbPath + "]");
        try {
        	for (int i = 0; i < threadCount; i++)
        		futures.add(executorService.submit(engineWriters.get(i)));
        	resultsFuture = Futures.successfulAsList(futures);
        	resultsFuture.get();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        long writeTime = System.currentTimeMillis();
        fillRandomIOPS = 1000 * operationSum /(writeTime - clearWritePageCacheTime);
        logger.error("fillrandom cost:" + (writeTime - clearWritePageCacheTime) + "ms, IOPS:" + fillRandomIOPS + "ops/second");
        
        restartEngine(engine, dbPath);
        
        //random read
        logger.error("Read random");
		logger.error("Drop caches...");
        BenchMarkUtil.dropPageCache();
        long clearReadPageCacheTime = System.currentTimeMillis();
        logger.error("Drop caches done, used:			" + (clearReadPageCacheTime - writeTime) + "ms");
        logger.error("DB path:[" + dbPath + "]");
        try {
        	futures = Lists.newArrayList();
        	long readSeed = 0;
        	for (int i = 0; i < threadCount; i++) {
        		futures.add(executorService.submit(new EngineReader(writeCount, engine, keyLen, operationSum, readSeed++)));
        	}
        	resultsFuture = Futures.successfulAsList(futures);
        	List<Integer> readSuccess = resultsFuture.get();
        	int readSuccessSum = 0;
        	for (Integer readSucces : readSuccess) {
        		readSuccessSum += readSucces;
			}
        	logger.error("read success (" + readSuccessSum + " of " + (threadCount * writeCount) + "found )");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        
        long readTime = System.currentTimeMillis();
        readRandomIOPS = 1000 * operationSum /(readTime - clearReadPageCacheTime);
        logger.error("readrandom cost:" + (readTime - clearReadPageCacheTime) + "ms, IOPS:" + readRandomIOPS + "ops/second");
       
        restartEngine(engine, dbPath);
        
        //seq read
        logger.error("Read seq");
		logger.error("Drop caches...");
        BenchMarkUtil.dropPageCache();
        long clearForeachPageCacheTime = System.currentTimeMillis();
        logger.error("Drop caches done, used:			" + (clearForeachPageCacheTime - readTime) + "ms");
        futures = Lists.newArrayList();
        DefaultVisitor visitor = new DefaultVisitor();
        try {
        	for (int i = 0; i < readSeqCount; i++) {
        		futures.add(executorService.submit(new EngineRanger(engine, visitor)));
        	}
        	resultsFuture = Futures.successfulAsList(futures);
            resultsFuture.get();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            executorService.shutdown();
            memCheckFuture.cancel(true);
            executor.shutdown();
            engine.close();
        }
        long foreachTime = System.currentTimeMillis();
        readSeqIOPS = (1000 * visitor.visitCount.get()) /(foreachTime - clearForeachPageCacheTime);
        logger.error("readseq cost:" + (foreachTime - clearForeachPageCacheTime) + "ms, IOPS:"  + readSeqIOPS + "ops/second visitCount:" + visitor.visitCount);
        
        logger.error("------------------------------------------------------------------------");
        logger.error("!!!Competition Report!!!");
        logger.error("	    Readseq:" + readSeqIOPS +  " ops/second");
        logger.error("	 Readrandom:" + readRandomIOPS + " ops/second");
        logger.error("	 Fillrandom:" + fillRandomIOPS +  " ops/second");
        DecimalFormat df = new DecimalFormat("0.00");
        logger.error("Time taken:" + df.format((float)(System.currentTimeMillis() - startTime) / 1000) + "s");
        
        
        logger.error("Max Memory:" + memChecker.getCurrentMemoryMb() + " MB");
        logger.error("  Max Disk:" + BenchMarkUtil.getDBPathSize(dbPath) + " MB");
        
	}

	private static void restartEngine(AbstractEngine engine, String path) {
		try {
			logger.error("restart engine...");
			long startTime = System.currentTimeMillis();
			logger.error("close engine...");
			engine.close();
			long closeTime = System.currentTimeMillis();
			logger.error("close engine done, used:" + (closeTime - startTime) + "ms");
			
			logger.error("open engine...");
			engine.open(path);
			long openTime = System.currentTimeMillis();
			logger.error("open engine done, used:" + (openTime - closeTime) + "ms");
		} 
		catch (EngineException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

}
