namespace IT4You.Application.FinanceAnalytics.DTOs
{
    public class FinanceSummaryDto
    {
        public decimal TotalContasPagarAberto { get; set; }
        public decimal TotalContasReceberAberto { get; set; }
        public decimal TotalPago { get; set; }
        public decimal TotalRecebido { get; set; }
        public decimal SaldoProjetado { get; set; }
    }
}
