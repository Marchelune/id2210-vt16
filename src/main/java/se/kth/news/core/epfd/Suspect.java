package se.kth.news.core.epfd;

import se.sics.kompics.KompicsEvent;
import se.sics.ktoolbox.util.network.KAddress;


public class Suspect implements KompicsEvent {

	public final KAddress suspected;

	public Suspect(KAddress suspected) {
		this.suspected = suspected;
	}


}
