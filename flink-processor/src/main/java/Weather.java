import java.util.Objects;

public class Weather {

  /*
  {
    "city": "New York",
    "temperature": "10.34"
  }
  */

  /*
  {
    "tagId": "",
    "channelId": "",
    "publisherId": "",
    "adsSourceId": "",
    "publisherChannelId": "",
    "connectionId": "",
    "inventory": 0,
    "timestamp": "2020-01-01T00:00:00.000Z",
  }
  */

  public String tagId;
  public String channelId;
  public String publisherId;
  public String adsSourceId;
  public String publisherChannelId;
  public String connectionId;

  public Weather() {}

  public Weather(String tagId, String channelId, String publisherId, String adsSourceId, String publisherChannelId, String connectionId) {
    this.tagId = tagId;
    this.channelId = channelId;
    this.publisherId = publisherId;
    this.adsSourceId = adsSourceId;
    this.publisherChannelId = publisherChannelId;
    this.connectionId = connectionId;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("Weather{");
    sb.append("tagId=").append(tagId).append('\'');
    sb.append(", channelId=").append(channelId).append('\'');
    sb.append(", publisherId=").append(publisherId).append('\'');
    sb.append(", adsSourceId=").append(adsSourceId).append('\'');
    sb.append(", publisherChannelId=").append(publisherChannelId).append('\'');
    sb.append(", connectionId=").append(connectionId).append('\'');

    // sb.append("city=").append(city).append('\'');
    // sb.append(", temperature=").append(String.valueOf(temperature)).append('\'');
    return sb.toString();
  }

  public int hashCode() {
    return Objects.hash(super.hashCode(), tagId, channelId, publisherId, adsSourceId, publisherChannelId, connectionId);
  }
}