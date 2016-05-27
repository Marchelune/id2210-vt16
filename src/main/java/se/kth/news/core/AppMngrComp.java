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
package se.kth.news.core;

import se.kth.news.core.epfd.Epfd;
import se.kth.news.core.epfd.EventuallyPerfectFailureDetectorPort;
import se.kth.news.core.leader.LeaderSelectComp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.news.core.leader.LeaderSelectPort;
import se.kth.news.core.news.NewsComp;
import se.kth.news.core.news.util.NewsViewComparator;
import se.kth.news.core.news.util.NewsViewGradientFilter;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;
import se.sics.ktoolbox.croupier.CroupierPort;
import se.sics.ktoolbox.gradient.GradientPort;
import se.sics.ktoolbox.omngr.bootstrap.BootstrapClientComp;
import se.sics.ktoolbox.overlaymngr.OverlayMngrPort;
import se.sics.ktoolbox.overlaymngr.events.OMngrTGradient;
import se.sics.ktoolbox.util.identifiable.Identifier;
import se.sics.ktoolbox.util.network.KAddress;
import se.sics.ktoolbox.util.overlays.view.OverlayViewUpdatePort;

/**
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class AppMngrComp extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(BootstrapClientComp.class);
    private String logPrefix = "";
    //*****************************CONNECTIONS**********************************
    Positive<OverlayMngrPort> omngrPort = requires(OverlayMngrPort.class);
    //***************************EXTERNAL_STATE*********************************
    private ExtPort extPorts;
    private KAddress selfAdr;
    private Identifier gradientOId;
    //***************************INTERNAL_STATE*********************************
    private Component leaderSelectComp;
    private Component newsComp;
    private Component epfd;
    //******************************AUX_STATE***********************************
    private OMngrTGradient.ConnectRequest pendingGradientConnReq;
    //**************************************************************************

    public AppMngrComp(Init init) {
        selfAdr = init.selfAdr;
        logPrefix = "<nid:" + selfAdr.getId() + ">";
        LOG.info("{}initiating...", logPrefix);

        extPorts = init.extPorts;
        gradientOId = init.gradientOId;

        subscribe(handleStart, control);
        subscribe(handleGradientConnected, omngrPort);
    }

    Handler handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.info("{}starting...", logPrefix);

            pendingGradientConnReq = new OMngrTGradient.ConnectRequest(gradientOId,
                    new NewsViewComparator(), new NewsViewGradientFilter());
            trigger(pendingGradientConnReq, omngrPort);
        }
    };

    Handler handleGradientConnected = new Handler<OMngrTGradient.ConnectResponse>() {
        @Override
        public void handle(OMngrTGradient.ConnectResponse event) {
            LOG.info("{}overlays connected", logPrefix);
            connectEPFD();
            connectLeaderSelect();
            connectNews();
            
            trigger(Start.event, leaderSelectComp.control());
            trigger(Start.event, newsComp.control());
            trigger(Start.event, epfd.control());
        }
    };
    
    private void connectEPFD(){
    	epfd = create(Epfd.class, new Epfd.Init(selfAdr));
    	connect(epfd.getNegative(Timer.class),  extPorts.timerPort, Channel.TWO_WAY);
    	connect(epfd.getNegative(Network.class), extPorts.networkPort, Channel.TWO_WAY);
    }


	private void connectLeaderSelect() {
        leaderSelectComp = create(LeaderSelectComp.class, new LeaderSelectComp.Init(selfAdr, new NewsViewComparator()));
        connect(leaderSelectComp.getNegative(Timer.class), extPorts.timerPort, Channel.TWO_WAY);
        connect(leaderSelectComp.getNegative(Network.class), extPorts.networkPort, Channel.TWO_WAY);
        connect(leaderSelectComp.getNegative(GradientPort.class), extPorts.gradientPort, Channel.TWO_WAY);
        connect(leaderSelectComp.getNegative(EventuallyPerfectFailureDetectorPort.class),
        		epfd.getPositive(EventuallyPerfectFailureDetectorPort.class), Channel.TWO_WAY);
    }

    private void connectNews() {
        newsComp = create(NewsComp.class, new NewsComp.Init(selfAdr, gradientOId));
        connect(newsComp.getNegative(Timer.class), extPorts.timerPort, Channel.TWO_WAY);
        connect(newsComp.getNegative(Network.class), extPorts.networkPort, Channel.TWO_WAY);
        connect(newsComp.getNegative(CroupierPort.class), extPorts.croupierPort, Channel.TWO_WAY);
        connect(newsComp.getNegative(GradientPort.class), extPorts.gradientPort, Channel.TWO_WAY);
        connect(newsComp.getNegative(LeaderSelectPort.class), leaderSelectComp.getPositive(LeaderSelectPort.class), Channel.TWO_WAY);
        connect(newsComp.getPositive(OverlayViewUpdatePort.class), extPorts.viewUpdatePort, Channel.TWO_WAY);
    }

    public static class Init extends se.sics.kompics.Init<AppMngrComp> {

        public final ExtPort extPorts;
        public final KAddress selfAdr;
        public final Identifier gradientOId;

        public Init(ExtPort extPorts, KAddress selfAdr, Identifier gradientOId) {
            this.extPorts = extPorts;
            this.selfAdr = selfAdr;
            this.gradientOId = gradientOId;
        }
    }

    public static class ExtPort {

        public final Positive<Timer> timerPort;
        public final Positive<Network> networkPort;
        public final Positive<CroupierPort> croupierPort;
        public final Positive<GradientPort> gradientPort;
        public final Negative<OverlayViewUpdatePort> viewUpdatePort;

        public ExtPort(Positive<Timer> timerPort, Positive<Network> networkPort, Positive<CroupierPort> croupierPort,
                Positive<GradientPort> gradientPort, Negative<OverlayViewUpdatePort> viewUpdatePort) {
            this.networkPort = networkPort;
            this.timerPort = timerPort;
            this.croupierPort = croupierPort;
            this.gradientPort = gradientPort;
            this.viewUpdatePort = viewUpdatePort;
        }
    }
}
