package se.kth.news.sim;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.kth.news.core.news.News;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.simulator.util.GlobalView;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.network.KAddress;

public class SimulationObserverTask1 extends ComponentDefinition {
private static final Logger LOG = LoggerFactory.getLogger(SimulationObserverTask1.class);
    
    Positive<Timer> timer = requires(Timer.class);
    Positive<Network> network = requires(Network.class);
    private UUID timerId;

    public SimulationObserverTask1() {

        subscribe(handleStart, control);
        subscribe(handleCheck, timer);
    }
    
    
    Handler<Start> handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            schedulePeriodicCheck();
        }
    };
    
    @Override
    public void tearDown() {
        trigger(new CancelPeriodicTimeout(timerId), timer);
    }
    
    Handler<CheckTimeout> handleCheck = new Handler<CheckTimeout>() {
        @Override
        public void handle(CheckTimeout event) {
        	GlobalView gv = config().getValue("simulation.globalview", GlobalView.class);
        	GlobalNewsStore newsStore = gv.getValue("simulation.newsstore", GlobalNewsStore.class);
        	AmountOfTrafficStore trafficStore = gv.getValue("simulation.amountOfTraffic", AmountOfTrafficStore.class);
        	
        	LinkedHashSet<News> globalNewsList = buildGlobalNewsList(newsStore);
        	//LOG.info(" Global News list : {}.\n", globalNewsList);
        	
        	newsCoverage(newsStore, globalNewsList);
        	nodeKnowledge(newsStore, globalNewsList);
        	amountOfTraffic(trafficStore);
        	
        }
    };
    
    private void schedulePeriodicCheck() {
        long period = config().getValue("simulation.checktimeout", Long.class);
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(period, period);
        CheckTimeout timeout = new CheckTimeout(spt);
        spt.setTimeoutEvent(timeout);
        trigger(spt, timer);
        timerId = timeout.getTimeoutId();
    }

    public static class CheckTimeout extends Timeout {

        public CheckTimeout(SchedulePeriodicTimeout spt) {
            super(spt);
        }
    }
    
    // Returns a Set of all the published news using the globalNewsStore
    public LinkedHashSet<News> buildGlobalNewsList(GlobalNewsStore newsStore){
    	Set<KAddress> nodeAddrs = newsStore.Store.keySet();
    	Iterator<KAddress> addrIt = nodeAddrs.iterator();
    	LinkedHashSet<News> globalNewsList = new LinkedHashSet<News>();
    	while(addrIt.hasNext()){
    		KAddress addr = addrIt.next();
    		LinkedHashSet<News> newsSet = newsStore.Store.get(addr);
    		globalNewsList.addAll(newsSet);
    	}
    	return globalNewsList;
    }
    
    // what percentage of nodes received each of the news items
    public void newsCoverage(GlobalNewsStore newsStore, LinkedHashSet<News> globalNewsList){
    	// Iterate on each new
    	Iterator<News> newsIt = globalNewsList.iterator();
    	
    	
    	// To compute the average news coverage
    	double avg = 0;
    	double totalNbNews = (double) globalNewsList.size();
    	
		Set<KAddress> nodeAddrs = newsStore.Store.keySet();
		double totalNbNodes = (double) nodeAddrs.size();
		
    	while(newsIt.hasNext()){
    		News currentNew = newsIt.next();
    		// Number of nodes who got the news
    		double nbNodes = 0;
    		// Iterate on each node

        	Iterator<KAddress> addrIt = nodeAddrs.iterator();
        	//LOG.info(" News Coverage: current news: '{}' ", currentNew);
        	while(addrIt.hasNext()){
        		KAddress addr = addrIt.next();
        		LinkedHashSet<News> newsSet = newsStore.Store.get(addr);
        		//LOG.info(" News Coverage: The node '{}' has the list {}.", addr.toString(), newsSet);
        		//LOG.info(" News Coverage : {}", newsSet.contains(currentNew));
        		if(newsSet.contains(currentNew)){
        			nbNodes++;
        		}
        		//LOG.info(" News Coverage : {}", nbNodes);
        	}

        	avg =  avg + nbNodes*100/totalNbNodes;  
        	//LOG.info("avg : {}", avg);
        	//LOG.info(" News Coverage: The news item '{}' was received by {} % of the nodes.\n", currentNew.toString(), nbNodes*100/totalNbNodes);
    	}
    	if(totalNbNews > 0){
    		avg = avg/totalNbNews;
    		LOG.info(" News Coverage : Total number of news {}.", totalNbNews);
    		LOG.info(" News Coverage: On average, news items were received by {} % of the nodes.\n", avg);
    	}
    }
    
    // for each node what percentage of news items did it see
    public void nodeKnowledge(GlobalNewsStore newsStore, LinkedHashSet<News> globalNewsList){
    	double totalNbNews = (double) globalNewsList.size();
    	
    	if(totalNbNews > 0){
    		// To compute the average node knowledge
    		double avg = 0;
    		Set<KAddress> nodeAddrs = newsStore.Store.keySet();
    		double totalNbNodes = nodeAddrs.size();
    		
    		// Iterate on each node
    		Iterator<KAddress> addrIt = nodeAddrs.iterator();
    		while(addrIt.hasNext()){
    			KAddress addr = addrIt.next();
    		
    			LinkedHashSet<News> newsSet = newsStore.Store.get(addr);
    			//LOG.info(" Node Knowledge: The node '{}' has the list {} .\n", addr.toString(), newsSet);
    			// Number of news the node has
    			double nbNews = (double) newsSet.size();
    			//LOG.info("list size : {}", nbNews);
    			avg =  avg + (nbNews*100/totalNbNews); 
    			//LOG.info("avg : {}", avg);
    			//LOG.info(" Node Knowledge: The node '{}' has received {} % of the news.\n", addr.toString(), (nbNews*100/totalNbNews));
    		}
    		avg = avg/totalNbNodes;
    		LOG.info(" Node Knowledge: Total number of nodes {}.", totalNbNodes);
    		LOG.info(" Node Knowledge: On average, nodes received {} % of the news. \n", avg);
    	}
    }
    
    
    // computes the global amount of traffic generated (sent msgs)
    public void amountOfTraffic(AmountOfTrafficStore trafficStore){
    	Integer globalTraffic = 0;
    	Set<KAddress> nodeAddrs = trafficStore.Store.keySet();
    	double totalNbNodes = (double) nodeAddrs.size(); 
    	Iterator<KAddress> addrIt = nodeAddrs.iterator();
		while(addrIt.hasNext()){
			KAddress addr = addrIt.next();
			Integer nodeTraffic = trafficStore.Store.get(addr);
			globalTraffic += nodeTraffic;
			//LOG.info(" Global Traffic: {} blabla.", nodeTraffic);
		}
		double globalTrafficDouble = (double) globalTraffic;
		LOG.info(" Global Traffic: Globally, nodes sent {} messages.", globalTrafficDouble);
		LOG.info(" Global Traffic: That is an average of {} messages per node. \n", globalTrafficDouble/totalNbNodes);
    }
}
