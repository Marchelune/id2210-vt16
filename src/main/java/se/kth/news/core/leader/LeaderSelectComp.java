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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.kth.news.core.news.util.NewsView;
import se.kth.news.core.news.util.NewsViewComparator;
import se.kth.news.sim.GlobalNewsStore;
import se.kth.news.sim.task2.MaxCvTimeStore;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.simulator.util.GlobalView;
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

	//*******************************CONNECTIONS********************************
	Positive<Timer> timerPort = requires(Timer.class);
	Positive<Network> networkPort = requires(Network.class);
	Positive<GradientPort> gradientPort = requires(GradientPort.class);
	Negative<LeaderSelectPort> leaderUpdate = provides(LeaderSelectPort.class);
	//*******************************EXTERNAL_STATE*****************************
	private KAddress selfAdr;
	//*******************************INTERNAL_STATE*****************************
	private Comparator<NewsView> viewComparator;
	private List<KAddress> currentNeighboursSample;
	private List<KAddress> currentFingersSample;

	private int stableConsecutiveRounds;
	private static final int STABILITY_ROUNDS_THRESHOLD = 5; //number of rounds before gradient is considered stable
	private static final double STABILITY_DISPARITY_THRESHOLD = 0.8;

	private NewsView currentLocalView;


	private KAddress currentLeader;

	private List<KAddress> quorum;
	private int receivedVotes;
	private boolean isCandidate;
	
	//Simulation
	// The maximum of rounds it took since the beginning of the simulation 
	// for the gradient to go from a non stable state to a stable state
	private Integer maxNbRoundsToConverge;
	// The current number of rounds since the last time the gradient was stable
	private Integer currentNbRoundsToConverge ;

	public LeaderSelectComp(Init init) {
		selfAdr = init.selfAdr;
		logPrefix = "<nid:" + selfAdr.getId() + ">";
		LOG.info("{}initiating...", logPrefix);

		viewComparator = init.viewComparator; // viewComparator=viewComparator ??
		currentLocalView = new NewsView(selfAdr.getId(), 0);
		currentLeader = null;

		stableConsecutiveRounds=0;
		
		//Simulation
		maxNbRoundsToConverge = 0;
		currentNbRoundsToConverge = 0;

		GlobalView gv = config().getValue("simulation.globalview", GlobalView.class);
		MaxCvTimeStore maxCvTimeStore = gv.getValue("simulation.maxCvTimeStore", MaxCvTimeStore.class);
		maxCvTimeStore.Store.put(selfAdr, maxNbRoundsToConverge);
		
		subscribe(handleStart, control);
		subscribe(handleGradientSample, gradientPort);
		subscribe(handleElectionRequest, networkPort);
		subscribe(handleVote, networkPort);
		subscribe(handleLeaderUpdate, networkPort);
		subscribe(handleLeaderPullRequest, networkPort);
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
		}
	};

	Handler handleGradientSample = new Handler<TGradientSample<NewsView>>() {
		@Override
		public void handle(TGradientSample<NewsView> sample) {
			List<Container<KAddress, NewsView>> newNeighboursSample = sample.getGradientNeighbours();
			currentLocalView = sample.selfView;

			//TODO fingers update            
			currentFingersSample = new ArrayList<>();
			for( Container<KAddress, ?> fg : sample.gradientFingers){
				currentFingersSample.add(fg.getSource());
			}

			
			
			
			
			currentNbRoundsToConverge++;
			
			if(! gradientIsStable(newNeighboursSample)) return; //gradient not stable to perform election
			
			// Simulation
			if (currentNbRoundsToConverge > maxNbRoundsToConverge){
				maxNbRoundsToConverge = currentNbRoundsToConverge;
				GlobalView gv = config().getValue("simulation.globalview", GlobalView.class);
				MaxCvTimeStore maxCvTimeStore = gv.getValue("simulation.maxCvTimeStore", MaxCvTimeStore.class);
				maxCvTimeStore.Store.put(selfAdr, maxNbRoundsToConverge);
			}
			currentNbRoundsToConverge = 0;
			
			if(!(currentLeader==null)) return; //already a leader, no need to check TODO maybe change the order later
			
			for (Container<KAddress, NewsView> neighbour : newNeighboursSample){
				if(viewComparator.compare(neighbour.getContent(), currentLocalView ) > 0 ){
					leaderPull(neighbour.getSource()); //maybe this good neighbour knows about a leader  
					return;
				}
			}
			
			//none of our neighbours is above us
			startElection();

		}
	};
	
	/**
	 * asks a neighbor to get the leader
	 */
	private void leaderPull(KAddress nb){
		LOG.debug("{} pulls leader from fingers", logPrefix);
		if(currentFingersSample.isEmpty())return;
		KHeader<KAddress> header = new BasicHeader<KAddress>(selfAdr,currentFingersSample.get(0), Transport.UDP);
		KContentMsg msg = new BasicContentMsg(header, new LeaderPullRequest());
		trigger(msg, networkPort);
	}

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
			if(currentLeader!=null) return;			
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
			}

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
			broadcastToNodes(content, currentNeighboursSample);
			trigger(content,leaderUpdate);
		}
	};
	
	/**
	 * Pull strategy : some node wants to know if we know the leader
	 */
	ClassMatchedHandler<LeaderPullRequest, KContentMsg<?, ?, LeaderPullRequest>> handleLeaderPullRequest = new ClassMatchedHandler<LeaderPullRequest, KContentMsg<?,?,LeaderPullRequest>>() {
		@Override
		public void handle(LeaderPullRequest content, KContentMsg<?, ?, LeaderPullRequest> context) {
			if(currentLeader==null) return;
			LOG.debug("{} answers a leader pull request from node {}", logPrefix, context.getHeader().getSource().getId());
			KHeader<KAddress> header = new BasicHeader<KAddress>(selfAdr, context.getHeader().getSource(), Transport.UDP);
			KContentMsg msg = new BasicContentMsg(header, new LeaderUpdate(currentLeader));
			trigger(msg, networkPort);
		}
	};

	public static class Init extends se.sics.kompics.Init<LeaderSelectComp> {

		public final KAddress selfAdr;
		public final NewsViewComparator viewComparator;

		public Init(KAddress selfAdr, NewsViewComparator viewComparator) {
			this.selfAdr = selfAdr;
			this.viewComparator = viewComparator;
		}
	}
}
