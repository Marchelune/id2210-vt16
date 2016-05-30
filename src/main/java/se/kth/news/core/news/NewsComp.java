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
package se.kth.news.core.news;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.apache.log4j.nt.NTEventLogAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.news.core.leader.LeaderSelectPort;
import se.kth.news.core.leader.LeaderUpdate;
import se.kth.news.core.news.util.NewsView;
import se.kth.news.core.news.util.PullRequest;
import se.kth.news.play.Ping;
import se.kth.news.play.Pong;
import se.kth.news.sim.GlobalNewsStore;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.simulator.util.GlobalView;
import se.sics.kompics.timer.*;
import se.sics.ktoolbox.croupier.CroupierPort;
import se.sics.ktoolbox.croupier.event.CroupierSample;
import se.sics.ktoolbox.gradient.GradientPort;
import se.sics.ktoolbox.gradient.event.TGradientSample;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;
import se.sics.ktoolbox.util.other.Container;
import se.sics.ktoolbox.util.overlays.view.OverlayViewUpdate;
import se.sics.ktoolbox.util.overlays.view.OverlayViewUpdatePort;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class NewsComp extends ComponentDefinition {

	private static final Logger LOG = LoggerFactory.getLogger(NewsComp.class);
	private static final int PULL_PERIOD = 5000;
	/**
	 * SAFE_PUSH_NODES : Nmb of nodes the leader pushes it news directly to prevent loosing some
	 * in case of failure.
	 */
	private static final int SAFE_PUSH_NODES = 3;
	private String logPrefix = " ";

	//*******************************CONNECTIONS********************************
	Positive<Timer> timerPort = requires(Timer.class);
	Positive<Network> networkPort = requires(Network.class);
	Positive<CroupierPort> croupierPort = requires(CroupierPort.class);
	Positive<GradientPort> gradientPort = requires(GradientPort.class);
	Positive<LeaderSelectPort> leaderPort = requires(LeaderSelectPort.class);
	Negative<OverlayViewUpdatePort> viewUpdatePort = provides(OverlayViewUpdatePort.class);
	//*******************************EXTERNAL_STATE*****************************
	private KAddress selfAdr;
	private Identifier gradientOId;
	//*******************************INTERNAL_STATE*****************************
	private NewsView localNewsView;
	private List<Container<KAddress, NewsView>> currentNeighbours = new ArrayList<>();
	private ArrayList<KAddress> currentFingers = new ArrayList<>();
	private LinkedHashSet<News> newsChain;
	private KAddress leader;

	/**
	 * currentNewsTimestamp : this timestamp has two uses
	 * - if the node is the leader, it increments the timestamp to order the news he publishes
	 * - if the node is a simple peer, this is the timestamp of the next news the node is expecting to receive
	 */
	private int currentNewsTimestamp =0;
	
	private ArrayList<News> createdNewsBuffer; //storing news if we don't have a leader
	private ArrayList<News> bufferedNews; //news received but we have to fetch other before we put them in the blockchain

	//****SIMULATION
	private int simulatedNewsCount;
	private final static int BASE_TTL = 10;
	private int newsTimeOut;



	private static class  NewsTimeOut extends Timeout{
		protected NewsTimeOut(SchedulePeriodicTimeout request) {
			super(request);
		}
	}
	
	private static class  PullTimeOut extends Timeout{
		protected PullTimeOut(SchedulePeriodicTimeout request) {
			super(request);
		}
	}

	public NewsComp(Init init) {
		selfAdr = init.selfAdr;
		logPrefix = "<nid:" + selfAdr.getId() + ">";
		LOG.info("{}initiating...", logPrefix);

		simulatedNewsCount = 0;
		newsTimeOut = config().getValue("newsTimeOut", Integer.class);
		createdNewsBuffer = new ArrayList<>();
		bufferedNews = new ArrayList<>();

		gradientOId = init.gradientOId;
		newsChain = new LinkedHashSet<>();
		leader=null;

		subscribe(handleStart, control);
		subscribe(handleCroupierSample, croupierPort);
		subscribe(handleGradientSample, gradientPort);
		subscribe(handleLeader, leaderPort);
		subscribe(handleNews, networkPort);
		subscribe(handlePullRequest, networkPort);
		subscribe(handleNewsTimer, timerPort);
		subscribe(handlePullTimeOut, timerPort);
	}

	Handler handleStart = new Handler<Start>() {
		@Override
		public void handle(Start event) {
			LOG.info("{}starting...", logPrefix);
			
			//-- News creation timer
			SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(5000, newsTimeOut);
			Timeout timeout = new NewsTimeOut(spt);
			spt.setTimeoutEvent(timeout);
			trigger(spt, timerPort);
			
			//-- News pull dissemination timer
			spt = new SchedulePeriodicTimeout(10000, PULL_PERIOD);
			timeout = new PullTimeOut(spt);
			spt.setTimeoutEvent(timeout);
			trigger(spt, timerPort);
			
			updateLocalNewsView();

			//GlobalView gv = config().getValue("simulation.globalview", GlobalView.class);
			//GlobalNewsStore newsStore = gv.getValue("simulation.newsstore", GlobalNewsStore.class);

			//Simulation
			//newsStore.Store.put(selfAdr, newsChain);
		}
	};

	/**
	 * simulation : generate a new news and broadcast it (sends it to the leader)
	 */
	Handler<NewsTimeOut> handleNewsTimer = new Handler<NewsComp.NewsTimeOut>() {
		@Override
		public void handle(NewsTimeOut event) {
			News newNews = new News("News " + simulatedNewsCount + " from " + selfAdr, BASE_TTL, -1);
			simulatedNewsCount++;
			LOG.debug("{} created new news:{}", logPrefix, newNews.toString());			
	
			createdNewsBuffer.add(newNews);
			if(leader==null) {
				LOG.debug("{} has no leader yet", logPrefix);
				return;
			}
			for(News n : createdNewsBuffer){
				KHeader<KAddress> header = new BasicHeader<KAddress>(selfAdr, leader, Transport.UDP);
				KContentMsg msg = new BasicContentMsg(header, n);
				trigger(msg, networkPort);
			}
		};
	};
	
	Handler<PullTimeOut> handlePullTimeOut = new Handler<NewsComp.PullTimeOut>() {
		@Override
		public void handle(PullTimeOut event) {
			if(isLeader()) return;
			KAddress peer = selectNodeFromNeighbours();
			if(peer==null) return;
			KHeader<KAddress> header = new BasicHeader<KAddress>(selfAdr, peer, Transport.UDP);
			KContentMsg msg = new BasicContentMsg(header, new PullRequest(currentNewsTimestamp));
			trigger(msg, networkPort);
		}
	};
	
	private KAddress selectNodeFromNeighbours(){
		if (currentNeighbours.isEmpty()) return null;
		//int n = (int) Math.random()*currentNeighbours.size() -1;
		int n=0;
		while(currentNeighbours.get(n).getContent().localNewsCount < currentNewsTimestamp+1){
			n++;
			if(n>=currentNeighbours.size()){
				return null;
			}
		}
		
		return currentNeighbours.get(n).getSource();
	}
	
	ClassMatchedHandler<PullRequest, KContentMsg<?, ?, PullRequest>> handlePullRequest = new ClassMatchedHandler<PullRequest, KContentMsg<?,?,PullRequest>>() {
		@Override
		public void handle(PullRequest content, KContentMsg<?, ?, PullRequest> context) {
			LOG.info("{} received a pull request from node {}", logPrefix,context.getHeader().getSource().getId());
			News[] toSend =  newsChain.toArray(new News[newsChain.size()]);
			KHeader<KAddress> header = new BasicHeader<KAddress>(selfAdr, context.getHeader().getSource(), Transport.UDP);
			for(int i = content.nmbReceivedNews;i<toSend.length;i++){
				KContentMsg msg = new BasicContentMsg(header, toSend[i]);
				trigger(msg, networkPort);
			}
		}
	};

	/**
	 * @return true is the node is the leader he knows
	 */
	private boolean isLeader(){
		if (leader==null) return false;
		return leader.equals(selfAdr);
	}

	private void updateLocalNewsView() {
		localNewsView = new NewsView(selfAdr.getId(), newsChain.size());
		LOG.debug("{}informing overlays of new view of size {}", logPrefix, newsChain.size());
		trigger(new OverlayViewUpdate.Indication<>(gradientOId, false, localNewsView.copy()), viewUpdatePort);
	}

	/**
	 * Used in task 1 to broadcast a news to neighbors.
	 * @param news
	 */
//	private void broadcastToNeighbours(News news){
//		for(KAddress neighbour : currentNeighbours ){
//			KHeader<KAddress> header = new BasicHeader<KAddress>(selfAdr, neighbour, Transport.UDP);
//			KContentMsg msg = new BasicContentMsg(header, news);
//			trigger(msg, networkPort);
//		}
//	}

	Handler handleCroupierSample = new Handler<CroupierSample<NewsView>>() {
		@Override
		public void handle(CroupierSample<NewsView> castSample) {
			//				-- TASK 1
			//				if (castSample.publicSample.isEmpty()) {
			//					return;
			//				}
			//				Iterator<Identifier> it = castSample.publicSample.keySet().iterator();
			//				currentNeighbours = new ArrayList<KAddress>();
			//				while(it.hasNext()){
			//					KAddress partner = castSample.publicSample.get(it.next()).getSource();
			//					currentNeighbours.add(partner);
			//				}
		}
	};

	Handler handleGradientSample = new Handler<TGradientSample<NewsView>>() {
		@Override
		public void handle(TGradientSample<NewsView> sample) {
			if (sample.gradientNeighbours.isEmpty()) return;

			currentNeighbours = sample.gradientNeighbours;
			
			currentFingers = new ArrayList<>();
			for(Container<KAddress, ?> nb : sample.gradientFingers){
				currentFingers.add(nb.getSource());
			}
		}
	};

	Handler handleLeader = new Handler<LeaderUpdate>() {
		@Override
		public void handle(LeaderUpdate event) {
			leader = event.leaderAdr;
		}
	};

	ClassMatchedHandler handleNews = new ClassMatchedHandler<News, KContentMsg<?, ?, News>>() {
		@Override
		public void handle(News content, KContentMsg<?, ?, News> context) {
			if(newsChain.contains(content)) return;
			//--Leader
			if(isLeader()){
				News newNews = new News(content.getTitle(),content.getTtl(),currentNewsTimestamp);
				
				//Task 4.1 pushing to the best node(s) after us in case we die
				// we MUST send to the best nodes first !!! 
				for(int k=0;k<SAFE_PUSH_NODES;k++){
					KHeader<KAddress> header = new BasicHeader<KAddress>(selfAdr, 
							currentNeighbours.get(currentNeighbours.size()-1-k).getSource(), Transport.UDP);
					KContentMsg msg = new BasicContentMsg(header, newNews);
					trigger(msg, networkPort);
				}
				
				//comiting after to ensure uniformity ... I guess
				currentNewsTimestamp++;
				newsChain.add(newNews);
				updateLocalNewsView();
				return;
			}
			
			
			//--Peer
			if(bufferedNews.contains(content)) return;				
			bufferedNews.add(content);

			for(News n : bufferedNews){
				if(currentNewsTimestamp  == n.getTimestamp()){
					if(!newsChain.contains(n)){
						newsChain.add(n);
						updateLocalNewsView();
					}
					currentNewsTimestamp++;
				}
			}
		}
	};

	ClassMatchedHandler handlePing
	= new ClassMatchedHandler<Ping, KContentMsg<?, ?, Ping>>() {

		@Override
		public void handle(Ping content, KContentMsg<?, ?, Ping> container) {
			LOG.info("{}received ping from:{}", logPrefix, container.getHeader().getSource());
			trigger(container.answer(new Pong()), networkPort);
		}
	};

	ClassMatchedHandler handlePong
	= new ClassMatchedHandler<Pong, KContentMsg<?, KHeader<?>, Pong>>() {
		@Override
		public void handle(Pong content, KContentMsg<?, KHeader<?>, Pong> container) {
			LOG.info("{}received pong from:{}", logPrefix, container.getHeader().getSource())  ;
		}
	};



	public static class Init extends se.sics.kompics.Init<NewsComp> {

		public final KAddress selfAdr;
		public final Identifier gradientOId;

		public Init(KAddress selfAdr, Identifier gradientOId) {
			this.selfAdr = selfAdr;
			this.gradientOId = gradientOId;
		}
	}
}


