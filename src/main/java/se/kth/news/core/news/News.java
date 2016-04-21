package se.kth.news.core.news;


/**
 * A piece of news to spread in the network.
 * @author remi
 *
 */
public class News {
	
	private final String title;
	private int ttl;
	
	public News(String title, int ttl) {
		this.title = title;
		this.ttl = ttl;
	}
	
	public News copyWithLowerTTL(){
		if(ttl==0) return null;
		return new News(title,ttl-1);
	}

	public String getTitle() {
		return title;
	}

	public int getTtl() {
		return ttl;
	}
	
	public boolean equals(News news){
		return title.equals(news.getTitle());
	}
	
	

}
