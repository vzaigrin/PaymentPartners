package pps.ppapp;

import java.io.Serializable;
import java.util.List;

public class Report implements Serializable {
    private List<String> head;
    private List<List<String>> data;

    public void setHead(List<String> head) {
        this.head = head;
    }

    public void setData(List<List<String>> data) {
        this.data = data;
    }

    public List<String> getHead() {
        return head;
    }

    public List<List<String>> getData() {
        return data;
    }
}
