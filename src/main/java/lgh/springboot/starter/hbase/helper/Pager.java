package lgh.springboot.starter.hbase.helper;

public class Pager {
    private final int page;
    private final int pageSize;

    public Pager(int page, int pageSize) {
        if (page < 1) {
            throw new IllegalArgumentException("page CANNOT be less than 1");
        }
        if (pageSize < 1) {
            throw new IllegalArgumentException("pageSize CANNOT be less than 1");
        }
        this.page = page;
        this.pageSize = pageSize;
    }

    public int getPage() {
        return page;
    }

    public int getPageSize() {
        return pageSize;
    }

    @Override
    public String toString() {
        return "Pager [page=" + page + ", pageSize=" + pageSize + "]";
    }

}
