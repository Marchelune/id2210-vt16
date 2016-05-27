package se.kth.news.core.epfd;

import se.sics.kompics.KompicsEvent;
import se.sics.ktoolbox.util.network.KAddress;

public class Restore implements KompicsEvent {

	public final KAddress restored;

	public Restore(KAddress restored) {
		this.restored = restored;
	}

	
}
