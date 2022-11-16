package kafka.example;

public class VehicleCoordinates {
    private  int vehicleId;
    private double longitude;
    private double latitude;

    public VehicleCoordinates(int vehicleId, double longitude, double latitude) {
        this.vehicleId = vehicleId;
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public VehicleCoordinates() {
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
}
