package de.tum.viewmaintenance.client;

import org.apache.cassandra.dht.LongToken;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MurmurHash;

import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * Created by anarchy on 6/11/15.
 */
public class GenerateTokenMurmur {
    public static void main(String[] args) {
        // 4937427 | -2215053331334018250 : Cassandra output
        System.out.println(generateRandomTokenMurmurPartition(3307466));
    }

    static LongToken generateRandomTokenMurmurPartition(int randKeyGenerated) {
        BigInteger bigInt = BigInteger.valueOf(randKeyGenerated);
        Murmur3Partitioner murmur3PartitionerObj = new Murmur3Partitioner();
        Token.TokenFactory tokenFac = murmur3PartitionerObj.getTokenFactory();
        org.apache.cassandra.dht.LongToken generatedToken = murmur3PartitionerObj.getToken(ByteBufferUtil.bytes(randKeyGenerated));
        return generatedToken;
    }

//    public static LongToken genToken(String rowKey) {
//        ByteBuffer key = ByteBufferUtil.bytes(rowKey);
//        long[] hash1 = new long[2];
//        long hash = MurmurHash.hash3_x64_128(key, key.position(), key.remaining(), hash1)[0];
//        LongToken lk = new LongToken(normalize(hash));
//        return lk;
//    }

    private static long[] getHash(ByteBuffer key)
    {
        long[] hash = new long[2];
        MurmurHash.hash3_x64_128(key, key.position(), key.remaining(), 0, hash);
        return hash;
    }

    private static long normalize(long v)
    {
        // We exclude the MINIMUM value; see getToken()
        return v == Long.MIN_VALUE ? Long.MAX_VALUE : v;
    }
}
