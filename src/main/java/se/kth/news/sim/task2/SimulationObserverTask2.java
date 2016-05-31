package se.kth.news.sim.task2;

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

public class SimulationObserverTask2 extends ComponentDefinition {
private static final Logger LOG = LoggerFactory.getLogger(SimulationObserverTask2.class);
    
    Positive<Timer> timer = requires(Timer.class);
    Positive<Network> network = requires(Network.class);
    private UUID timerId;
    private boolean leaderElected = false;

    public SimulationObserverTask2() {

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
        	MaxCvTimeStore maxCvTimeStore = gv.getValue("simulation.maxCvTimeStore", MaxCvTimeStore.class);
        	MaxCvTimeStore gotLeaderStore = gv.getValue("simulation.gotLeaderStore", MaxCvTimeStore.class);
        	MaxCvTimeStore nbPullRoundsStore = gv.getValue("simulation.nbPullRoundsStore", MaxCvTimeStore.class);
        	MaxCvTimeStore nbRoundsWhenLeaderElectStore = gv.getValue("simulation.nbRoundsWhenLeaderElectStore", MaxCvTimeStore.class);
        	//convergenceTime(maxCvTimeStore);
        	leaderInfoDissemination(gotLeaderStore, nbPullRoundsStore, nbRoundsWhenLeaderElectStore);
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
    

    // Displays the average maximum convergence time (in number of rounds) of the gradient
    public void convergenceTime(MaxCvTimeStore maxCvTimeStore){
        
    	// Number of nodes for which the gradient never converged
    	Integer neverCv = 0;
    	
    	double avg = 0;
    	Set<KAddress> nodeAddrs = maxCvTimeStore.Store.keySet();
    	double totalNbNodes = (double) nodeAddrs.size();
    	// Iterate on each node
    	Iterator<KAddress> addrIt = nodeAddrs.iterator();
    	while(addrIt.hasNext()){
    		KAddress addr = addrIt.next();
    		
    		// Max convergence time for the current node
    		Integer maxCvTimeNode = maxCvTimeStore.Store.get(addr);
    		if(maxCvTimeNode < 5){ // cf STABILITY_ROUNDS_THRESHOLD = 5 in LeaderSelectComp
    			neverCv++;
    		}
    		LOG.info(" Convergence Time: The gradient for the node '{}' converged in maximum {} rounds .\n", addr.toString(), maxCvTimeNode);

    		avg =  avg + (double) maxCvTimeNode; 
    	}
		avg = avg/totalNbNodes;
		LOG.info(" Convergence Time: Total number of nodes {}.", totalNbNodes);
		LOG.info(" Convergence Time: For {} nodes, the gradient has never converged \n", neverCv);
		LOG.info(" Convergence Time: On average, the gradient for the nodes converged in maximum {} rounds \n", avg);
    }
    
    // Displays the average and maximum dissemination time of the leader information (in number of pull rounds)
    public void leaderInfoDissemination(MaxCvTimeStore gotLeaderStore, MaxCvTimeStore nbPullRoundsStore, MaxCvTimeStore nbRoundsWhenLeaderElectStore){
    	
    	boolean allNodesHaveLeader = true;
    	double avg = 0;
    	double max = 0;
    	double nbNodesWithLeader = 0;
    	Set<KAddress> nodeAddrs = gotLeaderStore.Store.keySet();
    	double totalNbNodes = (double) nodeAddrs.size();
    	// Iterate on each node
    	Iterator<KAddress> addrIt = nodeAddrs.iterator();
    	while(addrIt.hasNext()){
    		KAddress addr = addrIt.next();
    		Integer numberOfPullsUntilGetLeader = gotLeaderStore.Store.get(addr);
    		
    		if (numberOfPullsUntilGetLeader < 0){ // if the node does not have a leader yet
    			allNodesHaveLeader = false;
    		} else {
    			double nbRoundsWhenLeaderElected = (double) nbRoundsWhenLeaderElectStore.Store.get(addr);
    			double disseminationTime = (double) numberOfPullsUntilGetLeader - nbRoundsWhenLeaderElected;
    			//LOG.info(" Leader Dissemination: Node {}, dissemination time : {}", addr, disseminationTime);
    			avg = avg + disseminationTime;
    			if(max < disseminationTime){
    				max = disseminationTime;
    			}
    			nbNodesWithLeader++;
    		}
    		
    	}
    	
    	
    	if(nbNodesWithLeader > 0 && !leaderElected){ // When the leader gets elected, the information instantly gets propagated to its 10 gradient neighbours
    		LOG.info(" Leader Dissemination: LEADER ELECTED");
    		// We save the number of pull rounds for each node at this moment (when the leader just got elected)
    		nbRoundsWhenLeaderElectStore.Store.putAll(nbPullRoundsStore.Store);
    		leaderElected = true;
    	}
    	if(allNodesHaveLeader){
    		avg = avg/totalNbNodes;
    		LOG.info(" Leader Dissemination: All nodes have received the leader information.");
    		LOG.info(" Leader Dissemination: Total number of nodes : {}", totalNbNodes);
    		LOG.info(" Leader Dissemination:  Average dissemination time: {} pull rounds.", avg);
    		LOG.info(" Leader Dissemination:  Maximum dissemination time: {} pull rounds.", max);
    	} else {
    		LOG.info(" Leader Dissemination: At least one node does not have a leader yet. \n");
    	}
    }
}
