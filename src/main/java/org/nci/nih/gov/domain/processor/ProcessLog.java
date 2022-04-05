package org.nci.nih.gov.domain.processor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Hashtable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.nci.nih.gov.domain.dao.PersistToDb;
import org.nci.nih.gov.domain.model.Domain;


public class ProcessLog {
	
	ResolverUtil util = new ResolverUtil();
	PersistToDb persist = new PersistToDb();

	public Stream<String> readLogLine(String fileName) {
		
	    Path path;
	    Stream<String> lines = null;
		try {
			path = Paths.get(fileName);
		    lines = Files.lines(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
	    return lines;
	}



	private Domain makeEmptyDomain(String ip) {
		Domain domain = new Domain();
		domain.setIp(ip);
		domain.setTld(ResolverUtil.UNKNOWN_TLD);
		return domain;
		
	}

	public String getAndFilterLineIp(String line) {

	    	if(line == null 
	    			|| line.equals("") 
	    			|| isBot(line) 
	    			|| isHealthPing(line))
	    				{ return "NOIP";}
	    	return getIpFromLine(line);
	    }
	
	
    // Filter for health checker ping
    private boolean isHealthPing(String domain) {
    	if(domain.contains("ELB-HealthChecker") || domain.contains("nagios") || domain.contains("bitdiscovery")) { return true;}
    	return false;
	}
    
	// Filter for google bot entries in log.
    public boolean isBot(String line) {
    	if(line.contains("Googlebot") || line.contains("bingbot") 
    			|| line.contains("qwant") || line.contains("Nimbostratus-Bot") 
    			|| line.contains("CensysInspect") || line.contains("l9explore") || line.contains("bitdiscovery")
    			|| line.contains("l9tcpid") || line.contains("302"))
    	{System.out.println("Removing bot reference"); return true;}
    	return false;
    }
    
    public String getIpFromLine(String line) {
    	String ip = null;
        final String regex = 
        		"^([\\d\\/.]+)";
   
        final Pattern pattern = Pattern.compile(regex);
		final Matcher matcher = pattern.matcher(line);
		if(matcher.find()) {
			ip = matcher.group(1);
		}
    	return ip;
    }




	public void validateAndProcesstoDomain(String line) throws SQLException {
		String ip = getAndFilterLineIp(line);
		persist.getDomain(ip);
	}




}
