package se.kth.news.core.news;


/**
 * A piece of news to spread in the network.
 * @author remi
 *
 */
public class News {
	
	private final String title;
	private final int ttl;
	
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

	@Override
	public int hashCode() {
		return title.hashCode();
	}

}
