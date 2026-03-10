namespace IT4You.Application.FinanceAnalytics.DTOs
{
    public class TopDebtorDto
    {
        public string Cliente { get; set; } = string.Empty;
        public decimal ValorTotalEmAberto { get; set; }
        public int QuantidadeParcelas { get; set; }
    }
}
