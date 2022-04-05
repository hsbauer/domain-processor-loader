package org.nci.nih.gov.domain.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.nci.nih.gov.domain.model.Domain;
import org.nci.nih.gov.domain.processor.ProcessLog;
import org.nci.nih.gov.domain.processor.ResolverUtil;

public class PersistToDb {
	
	  ResolverUtil util = new ResolverUtil();
	  private static final String SQL_INSERT = "INSERT INTO DOMAIN (ip, tld) VALUES (?,?)";
	  private static final String SQL_QUERY = "SELECT tld FROM domain WHERE ip=(?)";

	
	public String checkForResolvedIP(String ip) throws SQLException {

		if(util.isNotIp(ip)) {
			System.out.println(ip = "IP: " + ip + " does not meet the standard of an ip, will not query or persist");
			return null;
		}
	
		PreparedStatement stmt = null;
		ResultSet rs = null;
		Connection conn = null;
		
		try {
			
			conn = DataSourceMySql.getMySQLDataSource().getConnection();
			stmt = conn.prepareStatement(SQL_QUERY);
			stmt.setString(1, ip);
			rs = stmt.executeQuery();
		    
		    return rs == null || rs.next() == false?null: rs.getString("tld");
		}
		catch (SQLException ex){
		    // handle any errors
		    System.out.println("SQLException: " + ex.getMessage());
		    System.out.println("SQLState: " + ex.getSQLState());
		    System.out.println("VendorError: " + ex.getErrorCode());
		}
		finally {
		    // it is a good idea to release
		    // resources in a finally{} block
		    // in reverse-order of their creation
		    // if they are no-longer needed

		    if (rs != null) {
		        try {
		            rs.close();
		        } catch (SQLException sqlEx) { } // ignore

		        rs = null;
		    }

		    if (stmt != null) {
		        try {
		            stmt.close();
		        } catch (SQLException sqlEx) { } // ignore

		        stmt = null;
		    }
		    if(conn != null) {
		        try {
		            conn.close();
		        } catch (SQLException sqlEx) { } // ignore
		    }
		}
		return rs.getString(0);
	}


	public void saveNewDomain(Domain domain) {
	

		        try (
		        	Connection conn = DataSourceMySql.getMySQLDataSource().getConnection();
		            PreparedStatement preparedStatement = conn.prepareStatement(SQL_INSERT)) {

		            preparedStatement.setString(1,domain.getIp());
		            preparedStatement.setString(2,domain.getTld());

		            int row = preparedStatement.executeUpdate();

		            // rows affected
		            System.out.println(row); //1

		        } catch (SQLException e) {
		            System.err.format("SQL State: %s\n%s", e.getSQLState(), e.getMessage());
		        } catch (Exception e) {
		            e.printStackTrace();
		        }

		
	}
	
	public Domain resolveFreshDomain(String ip) throws SQLException {


		//Try digg
		String tld = util.digg(ip);

		//digg can't find an ip, try whois
		if (tld == null || tld == ResolverUtil.UNKNOWN_TLD) {
		 tld = util.whois(ip);
		}

		//Save new domain object to database including those without a domain name
		Domain domain = new Domain();
		domain.setIp(ip);
		domain.setTld(tld);
		
		saveNewDomain(domain);
		
		
		//If we can't find it, Don't send a value to the controller, let it be empty
		if(tld == ResolverUtil.UNKNOWN_TLD) {return null;}
		return domain;
	}

	public Domain getDomain(String ip) throws SQLException {
		
		//unable to resolve ip return null
		if(ip.equals("NOIP")) {return null;}
		//check the database first
		String domain = checkForResolvedIP(ip);
		//else resolve from services
		if(domain == null) {
			return resolveFreshDomain(ip);
		}
	    //return no value to the controller if we can't find domain
		else if(domain.equals(ResolverUtil.UNKNOWN_TLD))
		{return null;}
		
		//returning the original domain 
		return util.createDomain(domain, ip);
	}

}
