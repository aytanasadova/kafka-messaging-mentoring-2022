package kafka.example;

public class TravelledDistance {
    private int vehicleId;
    private double distance;

    public TravelledDistance(int vehicleId, double distance) {
        this.vehicleId = vehicleId;
        this.distance = distance;
    }

    public int getVehicleId() {
        return vehicleId;
    }

    public void setVehicleId(int vehicleId) {
        this.vehicleId = vehicleId;
    }

    public double getDistance() {
        return distance;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }
}
