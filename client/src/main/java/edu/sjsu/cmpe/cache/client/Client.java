package edu.sjsu.cmpe.cache.client;

import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.*;

public class Client {
	
    private final int numberOfReplicas = 3;
	private final SortedMap<String, CacheServiceInterface> circle = new TreeMap<String, CacheServiceInterface>();
    private final HashFunction md5 = Hashing.md5();

	public Client(Collection<CacheServiceInterface> nodes) {
		for (CacheServiceInterface node : nodes) {
			add(node);
		}
	}

	public void add(CacheServiceInterface node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            String hashcode = md5.newHasher().putString(node.toString() + i, Charsets.UTF_8).hash().toString();
            circle.put(hashcode, node);
        }
	}

	public void remove(CacheServiceInterface node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            String hashcode = md5.newHasher().putString(node.toString()+i, Charsets.UTF_8).toString();
            circle.remove(hashcode);
        }
	}

	

	public static void main(String[] args) throws Exception {
		
		System.out.println("Starting the Cache in the Client...");
		
		CacheServiceInterface cache1 = new DistributedCacheService("http://localhost:3000");
		CacheServiceInterface cache2 = new DistributedCacheService("http://localhost:3001");
		CacheServiceInterface cache3 = new DistributedCacheService("http://localhost:3002");
		
		ArrayList<CacheServiceInterface> nodes = new ArrayList<CacheServiceInterface>();
		
		nodes.add(cache1);
		nodes.add(cache2);
		nodes.add(cache3);

		Client client = new Client(nodes);

        HashMap<Integer, String> map = new HashMap<Integer, String>();
        map.put(1, "a");
        map.put(2, "b");
        map.put(3, "c");
        map.put(4, "d");
        map.put(5, "e");
        map.put(6, "f");
        map.put(7, "g");
        map.put(8, "h");
        map.put(9, "i");
        map.put(10, "j");


        for(Integer key : map.keySet()){
            String value= map.get(key);
            CacheServiceInterface cache = client.consistentHash(key);
            cache.put(key,value);
        }
        for (Integer key : map.keySet()) {
            DistributedCacheService cache = (DistributedCacheService)client.consistentHash(key);
            System.out.println(cache.getCacheServerUrl() + ": " + key + "=>" + cache.get(key));
        }


        for(Integer key : map.keySet()){
            String value= map.get(key);
            CacheServiceInterface cache = client.rendezvousHash(key);
            cache.put(key,value);
        }

        for (Integer key : map.keySet()) {
            DistributedCacheService cache = (DistributedCacheService)client.rendezvousHash(key);
            System.out.println(cache.getCacheServerUrl() + ": " + key + "=>" + cache.get(key));
        }

	}
	
	public CacheServiceInterface consistentHash(long key) {
		if (circle.isEmpty()) {
			return null;
		}
		String hash = md5.newHasher().putLong(key).hash().toString();
		if (!circle.containsKey(hash)) {
			SortedMap<String, CacheServiceInterface> tailMap = circle.tailMap(hash);
			hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
		}
		return circle.get(hash);
	}

    public CacheServiceInterface rendezvousHash(long key) {
        long maxValue = Long.MIN_VALUE;
        CacheServiceInterface max = null;
        for (CacheServiceInterface node : circle.values()) {
            Long hash = md5.newHasher()
                    .putString(node.toString(), Charsets.UTF_8)
                    .putLong(key)
                    .hash().asLong();
            if (hash > maxValue) {
                max = node;
                maxValue = hash;
            }
        }
        return max;
    }

}
