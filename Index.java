import java.util.ArrayList;
import java.util.Objects;

public class Index {
    private final ArrayList<IndexEntry> entries = new ArrayList<>();

    public void addEntry(IndexEntry entry) {
        entries.add(entry);
    }

    public void updateEntry(IndexEntry entry) {
        entries.remove(entry);
        entries.add(entry);
    }

    public void removeEntry(String filename) {
        entries.removeIf(entry -> Objects.equals(entry.getFile().filename(), filename));
    }

    public void setState(String filename, int state) {
        for (IndexEntry i : entries) {
            if (Objects.equals(i.getFile().filename(), filename)) {
                i.setState(state);
            }
        }
    }

    public int getState(String filename) {
        for (IndexEntry i : entries) {
            if (Objects.equals(i.getFile().filename(), filename)) {
                return i.getState();
            }
        }

        return 0;
    }

    public IndexEntry getEntry(String filename) {
        for (IndexEntry i : entries) {
            if (Objects.equals(i.getFile().filename(), filename)) {
                return i;
            }
        }

        return null;
    }

    public boolean exists(String filename) {
        for (IndexEntry i : entries) {
            if (Objects.equals(i.getFile().filename(), filename)) {
                return true;
            }
        }

        return false;
    }

    public String list() {
        StringBuilder list = new StringBuilder();
        for (IndexEntry i : entries) {
            if (i.getState() == 2) list.append(" ").append(i.getFile().filename());
        }
        return list.toString();
    }

    public ArrayList<IndexEntry> getFiles() {
        return entries;
    }

    public boolean ready() {
        for (IndexEntry i : entries) {
            if (i.getState() != 2 || i.getState() != 3) return false;
        }
        return true;
    }
}