package model;

public class Pair {
    private String start;
    private String end;

    public Pair() {
    }

    public Pair(String start, String end) {
        this.start = start;
        this.end = end;
    }

    public String getStart() {
        return start;
    }

    public void setStart(String start) {
        this.start = start;
    }

    public String getEnd() {
        return end;
    }

    public void setEnd(String end) {
        this.end = end;
    }

    @Override
    public String toString() {
        return "Pair{" +
                "start='" + start + '\'' +
                ", end='" + end + '\'' +
                '}';
    }
}
