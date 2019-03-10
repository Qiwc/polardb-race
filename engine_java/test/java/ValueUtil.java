import java.util.ArrayList;
import java.util.List;


public class ValueUtil {

    public static List<byte[]> generateKeyValue(long key) {
        byte[] keyBytes = new byte[8];
        for (int ix = 0; ix < 8; ++ix) {
            int offset = 64 - (ix + 1) * 8;
            keyBytes[ix] = (byte) ((key >> offset) & 0xff);
        }
        List<byte[]> bytes = new ArrayList<byte[]>();
        bytes.add(keyBytes);
        bytes.add(generateValue(keyBytes));
        return bytes;
    }

    public static byte[] generateValue(byte[] key) {
        byte[] value = new byte[4 * 1024];
        int idx = 0;
        for (int i = 0; i < 4 * 1024; i++) {
           value[i] = key[idx++ % 8];
        }
        return value;
    }

    public static boolean isEqual(byte[] v1, byte[] v2) {
        if (v1.length != v2.length) {
            return false;
        }
        for (int i = 0; i < v1.length; i++) {
            if (v1[i] != v2[i]) {
                return false;
            }
        }
        return true;
    }
}
