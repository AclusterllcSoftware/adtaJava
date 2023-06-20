package aclusterllc.adta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class CommonHelper {
    static Logger logger = LoggerFactory.getLogger(CommonHelper.class);
    public static String getStackTraceString(Exception ex){
        StringWriter errors = new StringWriter();
        ex.printStackTrace(new PrintWriter(errors));
        return errors.toString();
    }
    public static long bytesToLong(byte[] bytes)
    {
        return new BigInteger(bytes).longValue();
//        byte[] fillArray = new byte[Long.BYTES - bytes.length];
//        byte[] longArray = joinTwoBytesArray(fillArray, bytes);
//        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
//        buffer.put(longArray);
//        buffer.flip();//need flip
//        return buffer.getLong();
    }

    public static int bytesToInt(byte[] bytes)
    {
        return new BigInteger(bytes).intValue();
    }
    public static byte[] longToBytes(long x, int byteLength) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        byte[] wordArray = buffer.array();
        byte[] requiredByteArray = new byte[byteLength];
        int wordArrayLength = wordArray.length;
        int ignoredLength = wordArrayLength - byteLength;
        for (int i=0; i < wordArrayLength; i++) {
            if(i > (ignoredLength - 1)) {
                requiredByteArray[i-ignoredLength] = wordArray[i];
            }
        }
        return requiredByteArray;
    }
    public static byte[] joinTwoBytesArray(byte[] a, byte[] b) {
        byte[] returnArray = new byte[a.length + b.length];
        ByteBuffer buff = ByteBuffer.wrap(returnArray);
        buff.put(a);
        buff.put(b);
        return buff.array();
    }
    public static byte[] bitsFromBytes(byte[] source,int group){
        byte[] bits=new byte[source.length*8];
        try{
            for(int i=0;i<source.length/group;i++){
                for(int j=0;j<group;j++) {
                    int byteIndex=i*group+j;
                    int bitIndex=(i*group+(group-j)-1)*8;
                    byte s=source[byteIndex];
                    for(int b=0;b<8;b++){
                        bits[bitIndex+b]= (byte) ((s>>b)&1);
                    }
                }
            }
        }
        catch (Exception ex){
            logger.error(getStackTraceString(ex));
        }
        return bits;

    }
}
