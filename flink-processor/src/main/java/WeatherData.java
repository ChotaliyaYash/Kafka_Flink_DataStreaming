public class WeatherData {
    public String tagId;
    public String channelId;
    public String publisherId;
    public String adsSourceId;
    public String publisherChannelId;
    public String connectionId;
    public Double inventory;
  
    public WeatherData(String tagId, String channelId, String publisherId, String adsSourceId, String publisherChannelId, String connectionId, Double inventory) {
        this.tagId = tagId;
        this.channelId = channelId;
        this.publisherId = publisherId;
        this.adsSourceId = adsSourceId;
        this.publisherChannelId = publisherChannelId;
        this.connectionId = connectionId;
        this.inventory = inventory;
    }

    @Override
    public String toString() {
        return "WeatherData{" +
                "tagId='" + tagId + '\'' +
                ", channelId='" + channelId + '\'' +
                ", publisherId='" + publisherId + '\'' +
                ", adsSourceId='" + adsSourceId + '\'' +
                ", publisherChannelId='" + publisherChannelId + '\'' +
                ", connectionId='" + connectionId + '\'' +
                ", inventory=" + inventory +
                '}';
    }
}
  