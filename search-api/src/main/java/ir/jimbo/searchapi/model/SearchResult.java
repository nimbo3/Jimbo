package ir.jimbo.searchapi.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SearchResult implements Serializable {
    private List<SearchItem> searchItemList;
    private long searchTime;

    public SearchResult() {
        searchItemList = new ArrayList<>();
        searchTime = 0;
    }

    public SearchResult(List<SearchItem> searchItemList, long searchTime) {
        this.searchItemList = searchItemList;
        this.searchTime = searchTime;
    }

    public List<SearchItem> getSearchItemList() {
        return searchItemList;
    }

    public void setSearchItemList(List<SearchItem> searchItemList) {
        this.searchItemList = searchItemList;
    }

    public long getSearchTime() {
        return searchTime;
    }

    public void setSearchTime(long searchTime) {
        this.searchTime = searchTime;
    }
}
