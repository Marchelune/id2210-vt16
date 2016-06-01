/*
 * 2016 Royal Institute of Technology (KTH)
 *
 * LSelector is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.kth.news.core.leader;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.kth.news.core.epfd.EventuallyPerfectFailureDetectorPort;
import se.kth.news.core.epfd.Suspect;
import se.kth.news.core.epfd.Watch;
import se.kth.news.core.news.util.NewsView;
import se.kth.news.core.news.util.NewsViewComparator;
import se.kth.news.sim.GlobalNewsStore;
import se.kth.news.sim.task2.MaxCvTimeStore;
import se.kth.news.sim.task4.leaderStore;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.simulator.util.GlobalView;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.gradient.GradientPort;
import se.sics.ktoolbox.gradient.event.TGradientSample;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;
import se.sics.ktoolbox.util.other.Container;

/**
 * @author Rémi Sormain
 */
public class LeaderSelectComp extends ComponentDefinition {

	private static final Logger LOG = LoggerFactory.getLogger(LeaderSelectComp.class);
	private String logPrefix = " ";
	private static final int LEADER_PULL_PERIOD = 1000;

	//*******************************CONNECTIONS********************************
	Positive<Timer> timerPort = requires(Timer.class);
	Positive<Network> networkPort = requires(Network.class);
	Positive<GradientPort> gradientPort = requires(GradientPort.class);
	Positive<EventuallyPerfectFailureDetectorPort> failureDetector = requires(EventuallyPerfectFailureDetectorPort.class);
	Negative<LeaderSelectPort> leaderUpdate = provides(LeaderSelectPort.class);
	//*******************************EXTERNAL_STATE*****************************
	private KAddress selfAdr;
	//*******************************INTERNAL_STATE*****************************
	private Comparator<NewsView> viewComparator;
	private List<KAddress> currentNeighboursSample;
	private List<KAddress> currentFingersSample;

	private int stableConsecutiveRounds;
	private static final int STABILITY_ROUNDS_THRESHOLD = 5; //number of rounds before gradient is considered stable
	private static final double STABILITY_DISPARITY_THRESHOLD = 0.8; //% of similarity between 2 consecutive samples that is stable

	private NewsView currentLocalView;

	private KAddress currentLeader;

	private List<KAddress> quorum;
	private int receivedVotes;
	private boolean isCandidate;
	private boolean voteToken; //true if the node can vote for current election
	private UUID timerId;
	
	//Simulation
	// The maximum of rounds it took since the beginning of the simulation 
	// for the gradient to go from a non stable state to a stable state
	private Integer maxNbRoundsToConverge;
	// The current number of rounds since the last time the gradient was stable
	private Integer currentNbRoundsToConverge ;

	// Case without churn: number of pull rounds until the node gets the leader info (Task 3.1)
	// Case without churn: nodes choose only one leader and don't change anymore
	private Integer numberOfPulls;
	private Integer numberOfPullsUntilGetLeader;

	public LeaderSelectComp(Init init) {
		selfAdr = init.selfAdr;
		logPrefix = "<nid:" + selfAdr.getId() + ">";
		LOG.info("{}initiating...", logPrefix);

		viewComparator = init.viewComparator; // viewComparator=viewComparator ??
		currentLocalView = new NewsView(selfAdr.getId(), 0);
		currentLeader = null;
		voteToken = true;

		stableConsecutiveRounds=0;
		
		//Simulation
		maxNbRoundsToConverge = 0;
		currentNbRoundsToConverge = 0;
		//Task 3.1
		numberOfPulls = 0;
		numberOfPullsUntilGetLeader = 0;

		GlobalView gv = config().getValue("simulation.globalview", GlobalView.class);
		MaxCvTimeStore maxCvTimeStore = gv.getValue("simulation.maxCvTimeStore", MaxCvTimeStore.class);
		MaxCvTimeStore gotLeaderStore = gv.getValue("simulation.gotLeaderStore", MaxCvTimeStore.class);
		MaxCvTimeStore nbRoundsWhenLeaderElectStore = gv.getValue("simulation.nbRoundsWhenLeaderElectStore", MaxCvTimeStore.class);
		MaxCvTimeStore nbPullRoundsStore = gv.getValue("simulation.nbPullRoundsStore", MaxCvTimeStore.class);
		leaderStore leaderStore = gv.getValue("simulation.leaderStore", leaderStore.class);
		MaxCvTimeStore leaderFailureStore = gv.getValue("simulation.leaderFailureStore", MaxCvTimeStore.class);
		
		try{
			maxCvTimeStore.Store.put(selfAdr, maxNbRoundsToConverge);
			// We put -1 in the store to indicate we do not have a leader yet (Task 3.1)
			gotLeaderStore.Store.put(selfAdr, -1);
			nbRoundsWhenLeaderElectStore.Store.put(selfAdr, 0);
			nbPullRoundsStore.Store.put(selfAdr, 0);
			leaderStore.Store.put(selfAdr, currentLeader);
			// We put -1 in the store to indicate we have not detected a leader failure yet (Task 4.1)
			leaderFailureStore.Store.put(selfAdr, -1);
		}catch(Exception e){
			e.printStackTrace();
		}

		
		
		subscribe(handleStart, control);
		subscribe(handleGradientSample, gradientPort);
		subscribe(handleElectionRequest, networkPort);
		subscribe(handleVote, networkPort);
		subscribe(handleLeaderUpdate, networkPort);
		subscribe(handleLeaderPullRequest, networkPort);
		subscribe(handleSuspectLeader, failureDetector);
		subscribe(handleLeaderPullTimeOut,timerPort);
	}

	/**
	 * Update the neighbours sample and check for stability in the gradient
	 * 
	 * @return true if the gradient is stable enough to try an election
	 */
	private boolean gradientIsStable(List<Container<KAddress, NewsView>> newNeighboursSample){

		ArrayList<KAddress> tempSample = new ArrayList<>();
		for(Container<KAddress, ?> nb : newNeighboursSample){
			tempSample.add(nb.getSource());
		}

		if(currentNeighboursSample==null || currentNeighboursSample.size() != tempSample.size() ) {
			stableConsecutiveRounds =0;
			currentNeighboursSample = tempSample;
			LOG.debug("{} says : Gradient not stable !", logPrefix);
			return false;
		}

		ArrayList<KAddress> difference = (ArrayList<KAddress>) tempSample.clone();
		difference.retainAll(currentNeighboursSample);
		double disparity = difference.size() / tempSample.size();
		if(disparity >= STABILITY_DISPARITY_THRESHOLD){
			stableConsecutiveRounds++;
		}else{
			stableConsecutiveRounds=0;
		}

		currentNeighboursSample = tempSample;
		return stableConsecutiveRounds >= STABILITY_ROUNDS_THRESHOLD ;
	}

	private void broadcastToNodes(Object election, List<KAddress> nodes){
		for(KAddress neighbour : nodes ){
			KHeader<KAddress> header = new BasicHeader<KAddress>(selfAdr, neighbour, Transport.UDP);
			KContentMsg msg = new BasicContentMsg(header, election);
			trigger(msg, networkPort);
		}
	}

	Handler handleStart = new Handler<Start>() {
		@Override
		public void handle(Start event) {
			LOG.info("{}starting...", logPrefix);
			//-- News pull leader timer
			SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(3000, LEADER_PULL_PERIOD);
			Timeout timeout = new LeaderPullTimeOut(spt);
			spt.setTimeoutEvent(timeout);
			trigger(spt, timerPort);
			timerId = timeout.getTimeoutId();
		}
	};
	
	/**
	 * Pulls leader from the first finger
	 */
	Handler<LeaderPullTimeOut> handleLeaderPullTimeOut = new Handler<LeaderSelectComp.LeaderPullTimeOut>() {
		@Override
		public void handle(LeaderPullTimeOut event) {
			if(currentFingersSample==null || currentFingersSample.isEmpty())return;
			LOG.debug("{} pulls leader from fingers", logPrefix);
			//LOG.debug(" {} current leader {}",logPrefix, currentLeader); // simu
			KHeader<KAddress> header = new BasicHeader<KAddress>(selfAdr,currentFingersSample.get(0), Transport.UDP);
			KContentMsg msg = new BasicContentMsg(header, new LeaderPullRequest());
			trigger(msg, networkPort);
			// Simulation (Task 3.1)
			numberOfPulls++;
			// We store the number of pulls
			GlobalView gv = config().getValue("simulation.globalview", GlobalView.class);
			MaxCvTimeStore nbPullRoundsStore = gv.getValue("simulation.nbPullRoundsStore", MaxCvTimeStore.class);
			nbPullRoundsStore.Store.put(selfAdr, numberOfPulls);
		}
	};

	Handler handleGradientSample = new Handler<TGradientSample<NewsView>>() {
		@Override
		public void handle(TGradientSample<NewsView> sample) {
			List<Container<KAddress, NewsView>> newNeighboursSample = sample.getGradientNeighbours();
			currentLocalView = sample.selfView;

			currentFingersSample = new ArrayList<>();
			for( Container<KAddress, ?> fg : sample.gradientFingers){
				currentFingersSample.add(fg.getSource());
			}
			

			currentNbRoundsToConverge++; //Simulation
			
			if(! gradientIsStable(newNeighboursSample)) return; //gradient not stable to perform election
			
			// Simulation
			if (currentNbRoundsToConverge > maxNbRoundsToConverge){
				maxNbRoundsToConverge = currentNbRoundsToConverge;
				GlobalView gv = config().getValue("simulation.globalview", GlobalView.class);
				MaxCvTimeStore maxCvTimeStore = gv.getValue("simulation.maxCvTimeStore", MaxCvTimeStore.class);
				maxCvTimeStore.Store.put(selfAdr, maxNbRoundsToConverge);
			}
			currentNbRoundsToConverge = 0;
			
			if(!(currentLeader==null)) return; //already a leader, no need to check
			
			for (Container<KAddress, NewsView> neighbour : newNeighboursSample){
				if(viewComparator.compare(neighbour.getContent(), currentLocalView ) > 0 ){  
					return;
				}
			}
			
			//none of our neighbours is above us
			startElection();
		}
	};
	

	/**
	 * triggered when the node believes he's the leader.
	 * Sends messages to its neighbours to be elected.
	 */
	private void startElection(){
		LOG.debug("{} start an election", logPrefix);
		quorum = currentNeighboursSample;
		receivedVotes = 0;
		isCandidate= true;
		broadcastToNodes(new Election(selfAdr,currentLocalView), quorum);
	}

	
	ClassMatchedHandler<Election, KContentMsg<?, ?, Election>> handleElectionRequest = new ClassMatchedHandler<Election, KContentMsg<?, ?, Election>>() {
		@Override
		public void handle(Election content, KContentMsg<?, ?, Election> context) {
			if(currentLeader!=null&&!voteToken) return;
			voteToken=false;
			KHeader<KAddress> header = new BasicHeader<KAddress>(selfAdr, context.getHeader().getSource(), Transport.UDP);
			KContentMsg msg = new BasicContentMsg(header, new Vote(content));
			trigger(msg, networkPort);
		}
	};

	/**
	 * The leader considers itself elected when half of the quorum replies.
	 */
	ClassMatchedHandler<Vote, KContentMsg<?, ?, Vote>> handleVote = new ClassMatchedHandler<Vote, KContentMsg<?,?,Vote>>() {
		@Override
		public void handle(Vote content, KContentMsg<?, ?, Vote> context) {
			if(!isCandidate) return;
			receivedVotes++;
			if( receivedVotes > (int) quorum.size()/2){
				currentLeader = selfAdr;
				broadcastToNodes(new LeaderUpdate(selfAdr),currentNeighboursSample);
				LOG.debug("{} is the new leader !", logPrefix);
				isCandidate =false;
				trigger(new LeaderUpdate(selfAdr),leaderUpdate);
				
				// Simulation (Task 3.1 : getLeader)
				numberOfPullsUntilGetLeader = numberOfPulls;
				GlobalView gv = config().getValue("simulation.globalview", GlobalView.class);
				MaxCvTimeStore gotLeaderStore = gv.getValue("simulation.gotLeaderStore", MaxCvTimeStore.class);
				gotLeaderStore.Store.put(selfAdr, numberOfPullsUntilGetLeader);
				
				// Task 4.1
				shareNewLeaderToGlobalView();
			}
		}
	};
	
	/**
	 * The leader is probably dead. This is only sent to nodes close to the (ex)leader that watched him
	 */
	private Handler<Suspect> handleSuspectLeader = new Handler<Suspect>() {
		@Override
		public void handle(Suspect event) {
			if(!event.suspected.equals(currentLeader)) return;
			currentLeader=null;
			
			// Task 4.1
			shareNewLeaderToGlobalView();
			shareDetectLeaderFailureGlobalView();

			//Close nodes to the leader were not pulling
			//So we set a new timer to pull, starting in long enough to avoid pulling the leader we just suspected
			//because our finger is too slow at detecting crashes.
			
			
			SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(200000, LEADER_PULL_PERIOD);
			Timeout timeout = new LeaderPullTimeOut(spt);
			spt.setTimeoutEvent(timeout);
			trigger(spt, timerPort);
			timerId = timeout.getTimeoutId();
			
			//We can vote again
			voteToken=true;
			
		}
	};

	/**
	 * Push strategy when receiving a new leader
	 */
	ClassMatchedHandler<LeaderUpdate, KContentMsg<?, ?, LeaderUpdate>> handleLeaderUpdate = new ClassMatchedHandler<LeaderUpdate, KContentMsg<?,?,LeaderUpdate>>() {
		@Override
		public void handle(LeaderUpdate content, KContentMsg<?, ?, LeaderUpdate> context) {
			if(currentLeader!=null && content.leaderAdr.equals(currentLeader)) return;
			LOG.info("{} knows a new leader, the node {}", logPrefix, content.leaderAdr.getId());
			currentLeader= content.leaderAdr;
			
			// Simulation (Task 3.1 : getLeader)
			numberOfPullsUntilGetLeader = numberOfPulls;
			GlobalView gv = config().getValue("simulation.globalview", GlobalView.class);
			MaxCvTimeStore gotLeaderStore = gv.getValue("simulation.gotLeaderStore", MaxCvTimeStore.class);
			gotLeaderStore.Store.put(selfAdr, numberOfPullsUntilGetLeader);
			
			if(context.getHeader().getSource().equals(currentLeader)){ 
				//the leader himself sent the update : we are a close neighbour and should watch him
				trigger(new Watch(currentLeader),failureDetector);
				//useless to pull fingers to watch for a new the leader
				if(timerId!=null) trigger(new CancelPeriodicTimeout(timerId), timerPort);
			}
			
			voteToken=true; //we can vote again
			if(currentNeighboursSample != null){
				broadcastToNodes(content, currentNeighboursSample); //UNCOMMENT
			}
			trigger(content,leaderUpdate);
			
			// Task 4.1
			shareNewLeaderToGlobalView();
		}
	};
	
	/**
	 * Pull strategy : some node wants to know if we know the leader
	 */
	ClassMatchedHandler<LeaderPullRequest, KContentMsg<?, ?, LeaderPullRequest>> handleLeaderPullRequest = new ClassMatchedHandler<LeaderPullRequest, KContentMsg<?,?,LeaderPullRequest>>() {
		@Override
		public void handle(LeaderPullRequest content, KContentMsg<?, ?, LeaderPullRequest> context) {
			if(currentLeader==null) return;
			//LOG.debug("{} current leader {}", logPrefix, currentLeader); //simu
			LOG.debug("{} answers a leader pull request from node {}", logPrefix, context.getHeader().getSource().getId());
			KHeader<KAddress> header = new BasicHeader<KAddress>(selfAdr, context.getHeader().getSource(), Transport.UDP);
			KContentMsg msg = new BasicContentMsg(header, new LeaderUpdate(currentLeader));
			trigger(msg, networkPort);
		}
	};
	
	private static class  LeaderPullTimeOut extends Timeout{
		protected LeaderPullTimeOut(SchedulePeriodicTimeout request) {
			super(request);
		}
	}
	
	private void shareNewLeaderToGlobalView(){
		// Simulation - Task 4.1
		GlobalView gv = config().getValue("simulation.globalview", GlobalView.class);
		leaderStore leaderStore = gv.getValue("simulation.leaderStore", leaderStore.class);
		leaderStore.Store.put(selfAdr, currentLeader);
	}
	
	private void shareDetectLeaderFailureGlobalView(){
		// Simulation - Task 4.1
		GlobalView gv = config().getValue("simulation.globalview", GlobalView.class);
		MaxCvTimeStore leaderFailureStore = gv.getValue("simulation.leaderFailureStore", MaxCvTimeStore.class);
		leaderFailureStore.Store.put(selfAdr, 1);
	}

	public static class Init extends se.sics.kompics.Init<LeaderSelectComp> {

		public final KAddress selfAdr;
		public final NewsViewComparator viewComparator;

		public Init(KAddress selfAdr, NewsViewComparator viewComparator) {
			this.selfAdr = selfAdr;
			this.viewComparator = viewComparator;
		}
	}
}
