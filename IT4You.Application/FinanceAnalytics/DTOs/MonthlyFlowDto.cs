namespace IT4You.Application.FinanceAnalytics.DTOs
{
    public class MonthlyFlowDto
    {
        public string Mes { get; set; } = string.Empty;
        public decimal ValoresRecebidos { get; set; }
        public decimal ValoresPagos { get; set; }
        public decimal ValoresAVencer { get; set; }
    }
}
