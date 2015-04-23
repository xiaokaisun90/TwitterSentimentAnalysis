package mapred.localsentimentanalysis;

import java.util.Map;

public class Coordinate {
	/**
	 * Coordinate class to represent latitude and longitude
	 */
	private double lat, lon;
	
	public Coordinate(double lon, double lat) {
		this.lat = lat;
		this.lon = lon;
	}
	
	public double getLat() {
		return this.lat;
	}
	
	public double getLon() {
		return this.lon;
	}
	
	public double getDistance(Coordinate other) {
		double disLat = other.getLat() - this.getLat();
		double disLon = other.getLon() - this.getLon();
		return disLat * disLat + disLon * disLon;
	}
	
	public String getClosestCityName(Map<String, Coordinate> locationMap) {
		String closestLoc = "";
		double minDist = Double.MAX_VALUE;
		double curDist = 0.0;
		for (Map.Entry<String, Coordinate> entry: locationMap.entrySet()) {
			curDist = this.getDistance(entry.getValue());
			if (curDist < minDist)
			{
				closestLoc = entry.getKey();
				minDist = curDist;
			}
				
		}
		return closestLoc;
	}
}
