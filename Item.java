package A3.A3;

public class Item implements Comparable<Item>
{
	
	private String id;
	private double rating;
	
	public Item(String s, double d){
		id = s;
		rating = d;
	}
	public String getID(){
		return id;
	}
	public double getRating(){
		return rating;
	}
	public int compareTo(Item i) {
		if(this.getRating() > i.getRating()){
			return -1;
		}
		else if (this.getRating() < i.getRating()){
			return 1;
		}
		else {
			return 0;
		}
	}
}
