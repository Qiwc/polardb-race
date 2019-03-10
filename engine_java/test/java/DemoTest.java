import com.alibabacloud.polar_race.engine.common.EngineRace;
import junit.framework.TestCase;
import java.io.File;
import java.util.List;
import java.util.Random;

//import static org.hamcrest.CoreMatchers.containsString;
//import static org.junit.Assert.*;
//
//import org.junit.Test;


public class DemoTest extends TestCase {

    private static final int THREAD_NUM = 64;
    private static final int OPT_NUM_PER_THREAD = 1000;
    private static final String DATA_PATH = "data";

    private EngineRace engine = new EngineRace();

    public void testWrite() throws Exception {
        engine.open(DATA_PATH);
        Thread[] threads = new Thread[THREAD_NUM];

        // Write
        for (int i = 0; i < THREAD_NUM; i++) {
            final long seed = 10000000000L * i;
            threads[i] = new Thread(
                    new Runnable() {
                        public void run() {
                            try {
                                for (int i = 0; i < OPT_NUM_PER_THREAD; i++) {
                                    List<byte[]> keyValue = ValueUtil.generateKeyValue(seed + i);
                                    engine.write(keyValue.get(0), keyValue.get(1));
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
            );
        }
        for (int i = 0; i < THREAD_NUM; i++) {
            threads[i].start();
        }
        for (int i = 0; i < THREAD_NUM; i++) {
            threads[i].join();
        }

        engine.close();
        System.out.println("write finished");

    }


    public void testRandomRead() throws Exception {
        engine.open(DATA_PATH);
        Thread[] threads = new Thread[THREAD_NUM];

        // Read
        final Random random = new Random();
        for (int i = 0; i < THREAD_NUM; i++) {
            threads[i] = new Thread(
                    new Runnable() {
                        public void run() {
                            try {
                                for (int i = 0; i < OPT_NUM_PER_THREAD; i++) {
                                    long key = 10000000000L * random.nextInt(THREAD_NUM) + random.nextInt(OPT_NUM_PER_THREAD);
                                    List<byte[]> keyValue = ValueUtil.generateKeyValue(key);
                                    byte[] value = engine.read(keyValue.get(0));
                                    if (!ValueUtil.isEqual(keyValue.get(1), value)) {
                                        System.out.println("Wrong Value");
                                        System.exit(-1);
                                    }
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
            );
        }
        for (int i = 0; i < THREAD_NUM; i++) {
            threads[i].start();
        }
        for (int i = 0; i < THREAD_NUM; i++) {
            threads[i].join();
        }
        clear(DATA_PATH);
        System.out.println("read finished");
        engine.close();
    }

    private static boolean clear(String path) {
        File dirFile = new File(path);
        // 如果dir对应的文件不存在，则退出
        if (!dirFile.exists()) {
            return false;
        }

        if (dirFile.isFile()) {
            return dirFile.delete();
        } else {

            for (File file : dirFile.listFiles()) {
                clear(file.getPath());
            }
        }

        return dirFile.delete();
    }
}
