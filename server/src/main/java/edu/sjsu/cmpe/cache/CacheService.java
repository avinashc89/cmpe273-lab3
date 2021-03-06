package edu.sjsu.cmpe.cache;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;

import edu.sjsu.cmpe.cache.api.resources.CacheResource;
import edu.sjsu.cmpe.cache.config.CacheServiceConfiguration;
import edu.sjsu.cmpe.cache.domain.Entry;
import edu.sjsu.cmpe.cache.repository.CacheInterface;
import edu.sjsu.cmpe.cache.repository.ChronicleMapCache;

public class CacheService extends Service<CacheServiceConfiguration> {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private String serverName = "";
    
    CacheService(String serverName)
    {
    	this.serverName = serverName;
    }
    
    public static void main(String[] args) throws Exception {
    	
    	
    	System.out.println("Server name argument: " + args[1]);
    	new CacheService(args[1]).run(args);
    }

    @Override
    public void initialize(Bootstrap<CacheServiceConfiguration> bootstrap) {
        bootstrap.setName("cache-server");
    }

    @Override
    public void run(CacheServiceConfiguration configuration,
            Environment environment) throws Exception {
        
    	System.out.println(this.serverName);
    	ConcurrentHashMap<Long, Entry> map = new ConcurrentHashMap<Long, Entry>();
        CacheInterface cache = new ChronicleMapCache(this.serverName);
        environment.addResource(new CacheResource(cache));
        log.info("Loaded resources");

    }
}
