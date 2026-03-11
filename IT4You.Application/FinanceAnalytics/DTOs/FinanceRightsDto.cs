namespace IT4You.Application.FinanceAnalytics.DTOs
{
    public record FinanceRightsDto(
        bool HasPayableDashboardAccess,
        bool HasReceivableDashboardAccess,
        bool HasBankingDashboardAccess
    );
}
