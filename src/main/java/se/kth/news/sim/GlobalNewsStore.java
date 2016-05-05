package se.kth.news.sim;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;

import se.kth.news.core.news.News;
import se.sics.ktoolbox.util.network.KAddress;

public class GlobalNewsStore {

	public HashMap<KAddress, News[]> Store = new HashMap<KAddress, News[]>();
	
	
}
