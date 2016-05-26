package se.kth.news.core.news;


/**
 * A piece of news to spread in the network.
 * @author remi
 *
 */
public class News {
	
	private final String title;
	private final int ttl;
	private final int timestamp;
	
	public News(String title, int ttl, int timestamp) {
		this.title = title;
		this.ttl = ttl;
		this.timestamp = timestamp;
	}
	
	public News copyWithLowerTTL(){
		if(ttl==0) return null;
		return new News(title,ttl-1, timestamp);
	}

	public String getTitle() {
		return title;
	}

	public int getTtl() {
		return ttl;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(!(obj instanceof News)) return false;
		News news = (News) obj;
		return news.getTitle().equals(title);
	}

	@Override
	public String toString() {
		return title;
	}
	
	

	public int getTimestamp() {
		return timestamp;
	}

	@Override
	public int hashCode() {
		return title.hashCode();
	}

}
