package com.vulcan.sau.backend.route;

import org.apache.camel.builder.RouteBuilder;

import com.vulcan.sau.backend.BackendRouter;
import com.vulcan.sau.backend.processor.datatransfer.AggregateAllocationDataProcessor;
import com.vulcan.sau.backend.processor.datatransfer.DatatransferProcessor;
import com.vulcan.sau.backend.processor.datatransfer.SummarizeAllocationDataProcessor;
//import com.vulcan.sau.backend.processor.datatransfer.CrudProcessor;
import com.vulcan.sau.backend.processor.util.ComputeHighSeasGeo;
//import com.vulcan.sau.backend.processor.util.GeoMapper;

/**
 * EXAMPLE
 * 
 * Build routes for polling feeds or directories.
 * 
 */
public class DatatransferRouteBuilder extends RouteBuilder {

	DatatransferProcessor catProc = new DatatransferProcessor();
	ComputeHighSeasGeo highSeasProc = new ComputeHighSeasGeo();
	AggregateAllocationDataProcessor aggregateAllocationDataProc = new AggregateAllocationDataProcessor();
	SummarizeAllocationDataProcessor summarizeAllocationDataProc = new SummarizeAllocationDataProcessor();
    
    @Override
	public void configure() throws Exception
	{
		BackendRouter.getInjector().injectMembers(catProc);
		BackendRouter.getInjector().injectMembers(highSeasProc);
		BackendRouter.getInjector().injectMembers(aggregateAllocationDataProc);
		BackendRouter.getInjector().injectMembers(summarizeAllocationDataProc);
		
		/**
		 * Transfer data from source SQL Server DB to Postgres DB.
		 */
		from("activemq:queue:datatransfer.1.updateDatabase").id("UpdateDatabase")
			.onException(Exception.class)
				.handled(true)
				.setHeader("RouteName", constant("activemq:queue:datatransfer.1.updateDatabase"))
				.to("activemq:queue:util.PipelineExceptionHandler")
				.end()
			.process(catProc)
			.end();
		
		/**
		 * Summarize alloaction data to populate the allocation_result_xxxxx tables
		 */
		from("activemq:queue:datatransfer.2.summarizeAllocationData").id("SummarizeAllocationData")
			.onException(Exception.class)
				.handled(true)
				.setHeader("RouteName", constant("activemq:queue:datatransfer.2.summarizeAllocationData"))
				.to("activemq:queue:util.PipelineExceptionHandler")
				.end()
			.process(summarizeAllocationDataProc)
			.end();
		
		/**
		 * Transfer data from source SQL Server DB to Postgres DB.
		 */
		from("activemq:queue:datatransfer.3.aggregateAllocationData").id("AggregateAllocationData")
			.onException(Exception.class)
				.handled(true)
				.setHeader("RouteName", constant("activemq:queue:datatransfer.2.aggregateAllocationData"))
				.to("activemq:queue:util.PipelineExceptionHandler")
				.end()
			.process(aggregateAllocationDataProc)
			.end();
	}
}
