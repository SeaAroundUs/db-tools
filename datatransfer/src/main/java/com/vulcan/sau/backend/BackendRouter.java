package com.vulcan.sau.backend;

import javax.sql.DataSource;

import org.apache.camel.ProducerTemplate;
//import org.apache.log4j.Logger;

import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Module;
import com.google.inject.name.Names;

/**
 * Main class for Backend routing/processing. This starts up a Spring
 * ApplicationContext to handle dependency injection.
 * 
 */

public class BackendRouter extends RouterBase {
	private static BackendRouter singleton = new BackendRouter();
//	private static Logger logger = Logger.getLogger(BackendRouter.class);

	private DataSource _sauDataSource;

	public static void main(String args[]) throws Exception 
	{
		if(!singleton.initialized) singleton.init(RunMode.STAND_ALONE);
		singleton.startRouter(RunMode.STAND_ALONE);
	}

	public static void stop()
	{
		singleton.stopRouter();
	}

	public static boolean isRunning()
	{
		return singleton.running;
	}

	@Override
	protected void initDataSources() throws Exception {
		_sauDataSource = createDataSource("sau");
	}

	@Override
	protected void initializeInjector() {
		_guiceModules.add(new Module() {
			@Override
			public void configure(Binder binder) {
				/*
				 * Bind DataSources.
				 * MongoClient implements a connection pool under the covers, so binding to the one instance is sufficient.
				 */
				//binder.bind(MongoClient.class).annotatedWith(Names.named("mongo-action")).toInstance(mongoBind);
				binder.bind(DataSource.class).annotatedWith(Names.named("sau-ds")).toInstance(_sauDataSource);

				/*
				 *  Ideally, everyone in this VM should use the same ProducerTemplate, so we bind to a single instance here.
				 */
				binder.bind(ProducerTemplate.class).toInstance(_producerTemplate);
			}
		});
		injector = Guice.createInjector(_guiceModules);
	}
}
