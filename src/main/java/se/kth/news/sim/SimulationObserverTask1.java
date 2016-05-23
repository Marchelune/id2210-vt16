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
        	LinkedHashSet<News> globalNewsList = buildGlobalNewsList(newsStore);
        	LOG.info(" Global News list : {}.\n", globalNewsList);
        	newsCoverage(newsStore, globalNewsList);
        	nodeKnowledge(newsStore, globalNewsList);
        	
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
    	while(newsIt.hasNext()){
    		News currentNew = newsIt.next();
    		// Number of nodes who got the news
    		int nbNodes = 0;
    		// Iterate on each node
    		Set<KAddress> nodeAddrs = newsStore.Store.keySet();
    		int totalNbNodes = nodeAddrs.size();
        	Iterator<KAddress> addrIt = nodeAddrs.iterator();
        	while(addrIt.hasNext()){
        		KAddress addr = addrIt.next();
        		LinkedHashSet<News> newsSet = newsStore.Store.get(addr);
        		if(newsSet.contains(currentNew)){
        			nbNodes++;
        		}
        	}
        	LOG.info(" News Coverage: The news item '{}' was received by {} % of the nodes.\n", currentNew.toString(), (nbNodes/totalNbNodes)*100);
    	}
    }
    
    // for each node what percentage of news items did it see
    public void nodeKnowledge(GlobalNewsStore newsStore, LinkedHashSet<News> globalNewsList){
    	int totalNbNews = globalNewsList.size();
    	
    	// Iterate on each node
    	Set<KAddress> nodeAddrs = newsStore.Store.keySet();
    	Iterator<KAddress> addrIt = nodeAddrs.iterator();
    	while(addrIt.hasNext()){
    		KAddress addr = addrIt.next();
    		LinkedHashSet<News> newsSet = newsStore.Store.get(addr);
    		LOG.info(" Node Knowledge: The node '{}' has the list {} .\n", addr.toString(), newsSet);
    		// Number of news the node has
    		int nbNews = newsSet.size();
    		LOG.info(" Node Knowledge: The node '{}' has received {} % of the news.\n", addr.toString(), (nbNews*100/totalNbNews));
    	}
    }
}
