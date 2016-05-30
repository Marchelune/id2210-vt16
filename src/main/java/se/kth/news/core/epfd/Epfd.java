package se.kth.news.core.epfd;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.network.KContentMsg;
import se.sics.ktoolbox.util.network.KHeader;
import se.sics.ktoolbox.util.network.basic.BasicContentMsg;
import se.sics.ktoolbox.util.network.basic.BasicHeader;
import se.kth.news.core.news.NewsComp;
import se.kth.news.core.news.util.PullRequest;
import se.sics.kompics.ClassMatchedHandler;
import se.sics.kompics.network.Transport;


/**
 * 
 * Eventually Perfect Failure Detector component 
 * 
 * Provides strong completeness but only eventual strong accuracy
 * 
 * @author Rémi Sormain
 *
 */
public class Epfd extends ComponentDefinition {

	private static final Logger LOG = LoggerFactory.getLogger(Epfd.class);
	private String logPrefix = " ";
	private static final long INITIAL_DELAY = 3000;
	
	//*******************************EXTERNAL_STATE*****************************
	private KAddress selfAdr;
	
	//*******************************CONNECTIONS********************************
	private Positive<Timer> timerPort = requires(Timer.class);
	private Positive<Network> networkPort = requires(Network.class);
	private Negative<EventuallyPerfectFailureDetectorPort> epfd = provides(EventuallyPerfectFailureDetectorPort.class);
	
	//*******************************INTERNAL_STATE*****************************
	private KAddress leaderToWatch; 
	private long delay;
	private long deltaDelay;
	private boolean leaderAlive;
	private boolean leaderSuspected;
	private UUID timerId;
	
	
	public Epfd(Init init) {
		selfAdr = init.selfAdr;
		logPrefix = "<nid:" + selfAdr.getId() + ">";
		LOG.info("{} initiating...", logPrefix);
		
		subscribe(handleStart, control);
		subscribe(handleCheckTimeout, timerPort);
		subscribe(handleHeartbeatReplyMessage, networkPort);
		subscribe(handleHeartbeatRequestMessage, networkPort);
		subscribe(handleWatch, epfd);
		deltaDelay = 800;
	}
	
	
	
	private Handler<Start> handleStart = new Handler<Start>() {
		public void handle(Start event) {			
			delay = INITIAL_DELAY;
			leaderToWatch=null;
		}
	};
	
	/**
	 * set a new leader to watch
	 */
	private Handler<Watch> handleWatch = new Handler<Watch>() {
		@Override
		public void handle(Watch event) {
			if(leaderToWatch!=null) trigger(new CancelPeriodicTimeout(timerId), timerPort);
			
			leaderToWatch = event.leader;
			LOG.debug("{} is now watching the leader {}", logPrefix, leaderToWatch.getId());
			leaderAlive=true;
			leaderSuspected=false;
			
			SchedulePeriodicTimeout st = new SchedulePeriodicTimeout(0,delay);
			CheckTimeout timeout = new CheckTimeout(st);
			st.setTimeoutEvent(timeout);
			trigger(st, timerPort);
			timerId = timeout.getTimeoutId();		
		}
	};
	
	private Handler<Timeout> handleCheckTimeout = new Handler<Timeout>() {
		@Override
		public void handle(Timeout event) {
			if(!leaderAlive &&!leaderSuspected){
				leaderSuspected=true;
				trigger( new Suspect(leaderToWatch), epfd);
			}else if(leaderAlive && leaderSuspected){
				leaderSuspected=false;
				trigger(new Restore(leaderToWatch), epfd);
				
				//adapting the delay
				delay += deltaDelay;
				trigger(new CancelPeriodicTimeout(timerId), timerPort);
				SchedulePeriodicTimeout st = new SchedulePeriodicTimeout(0,delay);
				CheckTimeout timeout = new CheckTimeout(st);
				st.setTimeoutEvent(timeout);
				trigger(st, timerPort);
				timerId = timeout.getTimeoutId();
			}
			leaderAlive = false;			
			KHeader<KAddress> header = new BasicHeader<KAddress>(selfAdr, leaderToWatch, Transport.UDP);
			KContentMsg msg = new BasicContentMsg(header, new HeartbeatRequestMessage());
			trigger(msg, networkPort);

		}
	};
	
	ClassMatchedHandler<HeartbeatRequestMessage, KContentMsg<?, ?, HeartbeatRequestMessage>> handleHeartbeatRequestMessage = new ClassMatchedHandler<HeartbeatRequestMessage, KContentMsg<?,?,HeartbeatRequestMessage>>() {
		@Override
		public void handle(HeartbeatRequestMessage content, KContentMsg<?, ?, HeartbeatRequestMessage> context) {
			// TODO check if is leader ?
			KHeader<KAddress> header = new BasicHeader<KAddress>(selfAdr, context.getHeader().getSource(), Transport.UDP);
			KContentMsg msg = new BasicContentMsg(header, new HeartbeatReplyMessage());
			trigger(msg, networkPort);
		}
	}; 
	
	ClassMatchedHandler<HeartbeatReplyMessage, KContentMsg<?, ?, HeartbeatReplyMessage>> handleHeartbeatReplyMessage = new ClassMatchedHandler<HeartbeatReplyMessage, KContentMsg<?,?,HeartbeatReplyMessage>>() {
		@Override
		public void handle(HeartbeatReplyMessage content, KContentMsg<?, ?, HeartbeatReplyMessage> context) {
			if(!context.getHeader().getSource().equals(leaderToWatch)) return;
			leaderAlive=true;
		}
	};
	
	
	public class CheckTimeout extends Timeout {
		protected CheckTimeout(SchedulePeriodicTimeout request) {
			super(request);
		}
	}
	
	public static class Init extends se.sics.kompics.Init<Epfd> {
		public final KAddress selfAdr;
		public Init(KAddress selfAdr) {
			this.selfAdr = selfAdr;
		}
	}
}