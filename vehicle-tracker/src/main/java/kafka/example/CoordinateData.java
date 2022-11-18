package kafka.example;

import java.util.Objects;

public class CoordinateData {
    private int vehicleId;
    private double longitude;
    private double latitude;

    public CoordinateData() {
    }

    public CoordinateData(int vehicleId, double longitude, double latitude) {
        this.vehicleId = vehicleId;
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public int getVehicleId() {
        return vehicleId;
    }

    public void setVehicleId(int vehicleId) {
        this.vehicleId = vehicleId;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CoordinateData that)) return false;
        return vehicleId == that.vehicleId && Double.compare(that.longitude, longitude) == 0 && Double.compare(that.latitude, latitude) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(vehicleId, longitude, latitude);
    }

    @Override
    public String toString() {
        return "CoordinateData{" +
                "vehicleId=" + vehicleId +
                ", longitude=" + longitude +
                ", latitude=" + latitude +
                '}';
    }
}
