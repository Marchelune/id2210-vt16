package se.kth.news.sim;

import java.util.ArrayList;
import java.util.HashMap;

import se.kth.news.core.news.News;
import se.sics.ktoolbox.util.network.KAddress;

public class GlobalNewsStore {

	public HashMap<KAddress, ArrayList<News>> Store;
	
	public GlobalNewsStore (){
		Store = new HashMap<KAddress, ArrayList<News>>();
	}
}
