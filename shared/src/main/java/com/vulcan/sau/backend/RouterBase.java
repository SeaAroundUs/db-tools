package com.vulcan.sau.backend;

import java.util.ArrayList;
import java.util.Collection;

import javax.sql.DataSource;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.pool.PooledConnectionFactory;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jms.JmsConfiguration;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.SimpleRegistry;
import org.apache.log4j.Logger;
import org.postgresql.ds.PGPoolingDataSource;

import com.google.inject.Injector;
import com.google.inject.Module;

/**
 * Main class for routing/processing. This starts up a Spring
 * ApplicationContext to handle dependency injection.
 * 
 */

public abstract class RouterBase {
    private CamelContext _camelContext;
    protected ProducerTemplate _producerTemplate;
	private SimpleRegistry _registry;
	private static Logger logger = Logger.getLogger(RouterBase.class);
	
	protected boolean initialized = false;
	protected boolean running = false;

	protected static Injector injector;
	protected Collection<Module> _guiceModules = new ArrayList<Module>();

	protected abstract void initDataSources() throws Exception;

	protected abstract void initializeInjector();
	
	public void init(RunMode runmode) throws Exception 
	{
	    if(runmode == RunMode.STAND_ALONE)
        {
            System.setProperty("logPath", ".");
        }
        
        RouterConfig.init();
        _registry = new SimpleRegistry();

        initDataSources();

        _camelContext = new DefaultCamelContext(_registry);
        _producerTemplate = _camelContext.createProducerTemplate();

        // I want to initialize the Guice injection stuff here (after creating a camel context)
        // because I want to be able to @Inject the ProducerTemplate into components and routes.
        initializeInjector();
        
        // Create a connection pool for an ActiveMQ server.
        // Active MQ connection factory
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(RouterConfig.valFromBundles("activemq.endpoint"));

        // Pooling
        PooledConnectionFactory jmsConnectionFactory = new PooledConnectionFactory();
        jmsConnectionFactory.setMaxConnections(20);
        jmsConnectionFactory.setMaximumActiveSessionPerConnection(500);
        jmsConnectionFactory.setConnectionFactory(activeMQConnectionFactory);

        // Package as configuration
        JmsConfiguration jmsConfiguration = new JmsConfiguration();
        jmsConfiguration.setConnectionFactory(jmsConnectionFactory);
        
        org.apache.activemq.camel.component.ActiveMQComponent activemqComponent = new org.apache.activemq.camel.component.ActiveMQComponent();
        activemqComponent.setConfiguration(jmsConfiguration);
        
        _camelContext.addComponent("activemq", activemqComponent);
        
        /**
         * Using information in the app's configuration file (ex. default.properties)
         * instantiate and add RouteBuilders to the camel context.
         * 
         * List routes either by system property (ex. -Droutes=netflix,fandango)
         *  -- or --
         * as the value of route.active= within the properties file. 
         */
        String routesSystemProperty = System.getProperty("active.routes");
        String[] routesToAdd = routesSystemProperty != null ? routesSystemProperty.split(",") : RouterConfig.valFromBundles("active.routes").split(","); 
        
        for (String routeId : routesToAdd) {
            
            String clazz = RouterConfig.valFromBundles("route."+routeId.trim());
            
            RouteBuilder routeInstance = (RouteBuilder)Class.forName(clazz).newInstance();
            _camelContext.addRoutes(routeInstance);
            
            logger.info(String.format("Added '%s' route using '%s'", routeId, clazz));
        }
        
        logger.info("Initialized camel framework");
        initialized = true;
	}
	
	protected void startRouter(RunMode runmode) throws Exception 
    {
        if(initialized)
        {
            logger.info("Starting backend route");
            _camelContext.start();
            running = true;
    
            // if we're running as a standalone app, add a shutdown hook to do whatever is necessary 
            // to clean up and keep this thread running so the app doesn't exit
            if(runmode == RunMode.STAND_ALONE)
            {
                Runtime.getRuntime().addShutdownHook(new Thread(){@Override public void run() {stopRouter();}});
                while (true) {Thread.sleep(5000);}
            }
        }
	}
	
    protected void stopRouter()
	{
	    try 
        {
            System.out.println("Stopping ingestion.");
            _camelContext.stop();
            running = false;
        } 
        catch (Exception e) 
        {
            String s = String.format("Failed to stop CamelContext inside shutdown hook [%s].",e.getMessage());
            System.out.println(s);
        }
	}
	
	public void LoadRoute(String routeId) throws Exception
    {
        String clazz = RouterConfig.valFromBundles("route." + routeId.trim());
        
        RouteBuilder routeInstance = (RouteBuilder)Class.forName(clazz).newInstance();
        _camelContext.addRoutes(routeInstance);
        
        logger.info(String.format("Added '%s' route using '%s'", routeId, clazz));
    }
    
    public void UnloadRoute(String routeId) throws Exception
    {
        _camelContext.removeRoute(routeId);
        
        logger.info(String.format("Removed '%s' route", routeId));
    }
	
    protected DataSource createDataSource(String name) {
		PGPoolingDataSource ds = new PGPoolingDataSource();
		
		ds.setDataSourceName(name + "-datasource");
		ds.setDatabaseName(RouterConfig.valFromBundles(name + ".databaseName"));
		ds.setUser(RouterConfig.valFromBundles(name + ".user"));
		ds.setPassword(RouterConfig.valFromBundles(name + ".password"));
		ds.setServerName(RouterConfig.valFromBundles(name + ".serverName"));
		ds.setPortNumber(Integer.parseInt(RouterConfig.valFromBundles(name + ".portNumber")));
		ds.setMaxConnections(Integer.parseInt(RouterConfig.valFromBundles(name + ".maxConnections")));

		// Stuff into registry
		_registry.put(name, ds);

		return ds;
	}
    
	public static Injector getInjector() {
		return injector;
	}

	public static void injectMembers(Object objToInject) {
		injector.injectMembers(objToInject);
	}
}
