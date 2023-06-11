package edu.hanyang.services;

public class ServiceProvider {

    private static TokenizeService tokenizeService;
    private static ExternalSortService externalSortService;
    private static IndexService indexService;
    private static QueryProcessService queryProcessService;

    public static TokenizeService getTokenizeService() {
        if (tokenizeService == null)
            tokenizeService = new TokenizeService();
        return tokenizeService;
    }

    public static ExternalSortService getExternalSortService() {
        if (externalSortService == null)
            externalSortService = new ExternalSortService();
        return externalSortService;
    }

    public static IndexService getIndexService() {
        if (indexService == null)
            indexService = new IndexService();
        return indexService;
    }

    public static QueryProcessService getQueryProcessService() {
        if (queryProcessService == null)
            queryProcessService = new QueryProcessService();
        return queryProcessService;
    }
}
