package org.nci.nih.gov.domain.loader;

import java.sql.SQLException;
import java.util.Hashtable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.nci.nih.gov.domain.dao.DataSourceMySql;
import org.nci.nih.gov.domain.model.Domain;
import org.nci.nih.gov.domain.processor.ProcessLog;




public class DomainLoader {
	
	final BlockingQueue<String> queue = new LinkedBlockingQueue<String>(100);

	ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(1);
	ProcessLog processor = new ProcessLog();

	public static void main(String[] args) {
		new DomainLoader().run(args);
		//DataSourceMySql.testDataSource();

	}
	
	public void run(String ... args){
		String accessPath = "/Users/bauerhs/Desktop/access.log";



		Stream<String> lines = processor.readLogLine(args.length == 0?accessPath:args[0]);

		lines.forEach(x -> {

				blockingTimedQuery(x);

		});
		
	}



	public void blockingTimedQuery(String in) {

	scheduler.scheduleAtFixedRate(() -> { queue.offer(in);
	}, 0, 2, TimeUnit.SECONDS);

	        try {
				processor.validateAndProcesstoDomain(queue.take());
			} catch (InterruptedException e) {
				throw new RuntimeException(
						"Threaded concurrency error querying external resources", e);
			} catch (SQLException e) {
				throw new RuntimeException(
						"Data base query or update error", e);
			}

	}

}
