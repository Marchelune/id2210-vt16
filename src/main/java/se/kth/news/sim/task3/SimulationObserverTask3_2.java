package se.kth.news.sim.task3;

import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.kth.news.sim.task2.MaxCvTimeStore;
import se.kth.news.sim.task2.SimulationObserverTask2;
import se.kth.news.sim.task2.SimulationObserverTask2.CheckTimeout;
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

public class SimulationObserverTask3_2 extends ComponentDefinition  {
	private static final Logger LOG = LoggerFactory.getLogger(SimulationObserverTask3_2.class);
	
    Positive<Timer> timer = requires(Timer.class);
    Positive<Network> network = requires(Network.class);
    private UUID timerId;
	
    public SimulationObserverTask3_2() {

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
        	MaxCvTimeStore nbNewsStore = gv.getValue("simulation.nbNewsStore", MaxCvTimeStore.class);
        	newsDissemination(nbNewsStore);
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
    
    public void newsDissemination(MaxCvTimeStore nbNewsStore){
    	
    	// The maximum number of news items a node has in its news chain (leader)
    	double max = 0;
    	double avgNewsCov = 0;
    	double avgNodeKnow = 0;
    	
    	Set<KAddress> nodeAddrs = nbNewsStore.Store.keySet();
    	double totalNbNodes = (double) nodeAddrs.size();
    	// Iterate on each node
    	Iterator<KAddress> addrIt = nodeAddrs.iterator();
    	while(addrIt.hasNext()){
    		KAddress addr = addrIt.next();
    		// Get the number of news items the node has in its news chain
    		double nbNewsNode = (double) nbNewsStore.Store.get(addr);
    		if(nbNewsNode > max){
    			max = nbNewsNode;
    		}	
    	}
    	
    	// max contains the maximum number of news items a node has in its news chain (leader)
    	if(max > 0){
    		// contains the number of nodes that have each news item in their news chain, from number 0 to number max-1
    		double[] newsCoverage = new double[(int)max];
    		// Iterate on each node
    		addrIt = nodeAddrs.iterator();
        	while(addrIt.hasNext()){
        		KAddress addr = addrIt.next();
        		double nbNewsNode = (double) nbNewsStore.Store.get(addr);
        		// News Coverage
        		for(int i = 0; i< max; i++){ // for each news item (from 0 to max-1)
        			if(i <= nbNewsNode - 1){ // if the node has the news item number i
        				newsCoverage[i]++;
        			}
        		}
        		
        		// Node Knowledge
        		//LOG.info(" Node Knowledge: The node '{}' has received {} news items in its news chain.", addr.toString(), nbNewsNode);
        		//LOG.info(" Node Knowledge: The node '{}' has received {} % of the news items in its news chain.", addr.toString(), nbNewsNode*100/max);
        		avgNodeKnow = avgNodeKnow + nbNewsNode*100/max;
        	}
        	
        	// News coverage
        	for(int i = 0; i< max; i++){
        		//LOG.info(" News Coverage: The news item number '{}' was received by {} % of the nodes.", i, newsCoverage[i]*100/totalNbNodes);
        		avgNewsCov = avgNewsCov + newsCoverage[i]*100/totalNbNodes;
        	}
        	avgNewsCov = avgNewsCov/max;
        	LOG.info(" News Coverage : Total number of news {}.", max);
        	LOG.info(" News Coverage: On average, news items were received by {} % of the nodes.\n", avgNewsCov);
        	
        	// Node Knowledge
        	avgNodeKnow = avgNodeKnow/totalNbNodes;
        	LOG.info(" Node Knowledge: Total number of nodes {}.", totalNbNodes);
        	LOG.info(" Node Knowledge: On average, nodes received {} % of the news. \n", avgNodeKnow);
    	}
    	
    }
    
    
}
