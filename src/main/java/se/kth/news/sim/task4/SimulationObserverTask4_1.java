package se.kth.news.sim.task4;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.kth.news.sim.task2.MaxCvTimeStore;
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

public class SimulationObserverTask4_1 extends ComponentDefinition {
	private static final Logger LOG = LoggerFactory.getLogger(SimulationObserverTask4_1.class);

    Positive<Timer> timer = requires(Timer.class);
    Positive<Network> network = requires(Network.class);
    private UUID timerId;
    boolean firstLeaderElected = false;
    boolean firstLeaderFailed = false;
    boolean secondLeaderElected = false;
    HashSet<KAddress> SetNodeDetectedLeaderFailure = new HashSet<KAddress>();

    public SimulationObserverTask4_1() {

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
        	leaderStore leaderStore = gv.getValue("simulation.leaderStore", leaderStore.class);
        	MaxCvTimeStore leaderFailureStore = gv.getValue("simulation.leaderFailureStore", MaxCvTimeStore.class);
        	leaderFailure(leaderStore, leaderFailureStore);
        	
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
    
    public void leaderFailure(leaderStore leaderStore, MaxCvTimeStore leaderFailureStore){
    	Set<KAddress> nodeAddrs = leaderStore.Store.keySet();
    	double nbNodesWithLeader = 0;
    	KAddress currentLeader = null;
    	
    	double nbNodesDetectFailLeader = 0;
    	if(!firstLeaderElected){
    		// Iterate on each node
    		Iterator<KAddress> addrIt = nodeAddrs.iterator();
    		while(addrIt.hasNext()){
    			KAddress addr = addrIt.next();
    			KAddress nodeLeader = leaderStore.Store.get(addr);
    			if (nodeLeader != null){
    				nbNodesWithLeader++;
    				if(addr == nodeLeader){
    					currentLeader = addr;
    				}
    			}
    		}
    		if(nbNodesWithLeader > 0){
    			LOG.info("Leader Failure: First Leader '{}' Elected", currentLeader);
    			firstLeaderElected = true;
    		}
    	}
    	
    	if(firstLeaderElected){
    		nodeAddrs = leaderFailureStore.Store.keySet();
    		Iterator<KAddress> addrIt = nodeAddrs.iterator();
        	while(addrIt.hasNext()){
        		KAddress addr = addrIt.next();
        		Integer failureDetected = leaderFailureStore.Store.get(addr);
        		if(failureDetected > 0){
        			nbNodesDetectFailLeader++;
        			SetNodeDetectedLeaderFailure.add(addr);
        			
        			KAddress nodeLeader = leaderStore.Store.get(addr);
        			//LOG.info("Leader Failure: Node {} detected failure and has the leader {}", addr, nodeLeader);
        		}
        	}
        	// At least one node detected that the leader has failed
        	if(nbNodesDetectFailLeader > 0 && !firstLeaderFailed){
        		LOG.info("Leader Failure: First leader failed", currentLeader);
        		currentLeader = null;
        		firstLeaderFailed = true;
        	}
        	
        	// We iterate on the node which detected the failure
        	Iterator<KAddress> detectorsIt = SetNodeDetectedLeaderFailure.iterator();
        	while(detectorsIt.hasNext()){
        		KAddress detectorAddr = detectorsIt.next();
        		// Look at the leader for the corresponding node
        		KAddress nodeLeader = leaderStore.Store.get(detectorAddr);
        		if(nodeLeader != null && !secondLeaderElected){
        			currentLeader = nodeLeader;
        			secondLeaderElected = true;
        			LOG.info("Leader Failure: New leader {} elected ", currentLeader);
        		}
        	}
    	}
    }
}
