namespace IT4You.Application.Interfaces;

public interface IChatQueryPlugin
{
    void ClearExecutedQueries();
    void ClearExportMetadata();
    string? GetExecutedQueriesJson();

    string? LastExportId { get; }
    int LastExportTotalLinhas { get; }
    decimal LastExportValorTotal { get; }
    int AggregateTotalLinhas { get; }
    decimal AggregateValorTotal { get; }
}

