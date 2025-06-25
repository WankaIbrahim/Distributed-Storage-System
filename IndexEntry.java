import java.util.ArrayList;

public class IndexEntry {
    private final FileData fileData;
    private ArrayList<Integer> locations;
    private Integer state;

    public IndexEntry(FileData fileData, ArrayList<Integer> locations, Integer state) {
        this.locations = locations;
        this.fileData = fileData;
        this.state = state;
    }

    public ArrayList<Integer> getLocations() {
        return locations;
    }

    public void setLocations(ArrayList<Integer> locations) {
        this.locations = locations;
    }

    public FileData getFile() {
        return fileData;
    }

    public Integer getState() {
        return state;
    }

    public void setState(Integer state) {
        this.state = state;
    }
}
