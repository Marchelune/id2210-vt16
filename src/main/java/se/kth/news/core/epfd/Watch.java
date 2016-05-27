package se.kth.news.core.epfd;

import se.sics.kompics.KompicsEvent;
import se.sics.ktoolbox.util.network.KAddress;

public class Watch implements KompicsEvent {
	public final KAddress leader;

	public Watch(KAddress leader) {
		this.leader = leader;
	}
	

}
