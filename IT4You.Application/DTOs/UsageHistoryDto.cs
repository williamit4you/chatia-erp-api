namespace IT4You.Application.DTOs;

public record UsageHistoryDto(
    List<MonthlyUsageDto> MonthlyUsage,
    List<UserUsageDto> DetailedUsage
);

public record MonthlyUsageDto(
    string Month, // format e.g. "abr/26"
    int TotalCount,
    Dictionary<string, int> ModuleCounts
);

public record UserUsageDto(
    string UserName,
    int TotalCount,
    Dictionary<string, int> ModuleCounts
);
