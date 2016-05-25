package se.kth.news.core.leader;

import se.kth.news.core.news.util.NewsView;
import se.sics.ktoolbox.util.network.KAddress;

public class Election {
	public final KAddress leader;
	public final NewsView view;
	public Election(KAddress leader, NewsView view) {
		super();
		this.leader = leader;
		this.view = view;
	}
}
