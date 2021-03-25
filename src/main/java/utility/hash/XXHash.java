package utility.hash;

import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;
import utility.HashFunction;

public class XXHash implements HashFunction {
    private static final int hashSeed = 0x3747b28d;

    public int hf(Object input) {
        byte[] data = input.toString().getBytes();
        XXHashFactory factory = XXHashFactory.fastestInstance();
        XXHash32 hash32 = factory.hash32();
        int hash = hash32.hash(data, 0, data.length, hashSeed);
        if(hash<0) {
            hash *= -1;
        }
        return hash;
    }
}
