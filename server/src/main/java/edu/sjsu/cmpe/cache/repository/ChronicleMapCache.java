package edu.sjsu.cmpe.cache.repository;

import edu.sjsu.cmpe.cache.domain.Entry;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Avinash
 *
 */
public class ChronicleMapCache implements CacheInterface{

    ChronicleMapBuilder<Long, Entry> builder;
    ChronicleMap<Long, Entry> map;

    public ChronicleMapCache(String serverUrl){
        try {
        	String serverName = getServerName(serverUrl);
        	String fileName = serverName+"_log.dat";
        	File file = new File(fileName);
            builder = ChronicleMapBuilder.of(Long.class, Entry.class);
            map = builder.createPersistedTo(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
  
    /**
     * @param serverName
     * @return
     */
    private String getServerName (String serverName){
    	String serveryml = serverName.split("/")[1];
    	String[] server = serveryml.split("_");
    	return server[0] + "_" + server[1];
     }

    @Override
    public Entry save(Entry newEntry){
        checkNotNull(newEntry, "newEntry instance must not be null");
        map.putIfAbsent(newEntry.getKey(),newEntry);
        return newEntry;
    }

    @Override
    public Entry get(Long key) {

        checkArgument(key > 0,"Key was %s but expected greater than zero value", key);
        return map.get(key);
    }

    @Override
    public List<Entry> getAll() {
        return new ArrayList<Entry>(map.values());

    }



}