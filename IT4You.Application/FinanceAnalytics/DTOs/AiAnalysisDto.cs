using System.Collections.Generic;

namespace IT4You.Application.FinanceAnalytics.DTOs
{
    public class AiAnalysisDto
    {
        public decimal TotalReceberAberto { get; set; }
        public decimal TotalPagarAberto { get; set; }
        public decimal TotalRecebidoMes { get; set; }
        public decimal TotalPagoMes { get; set; }
        public IEnumerable<TopDebtorDto> TopDevedores { get; set; } = new List<TopDebtorDto>();
    }
}
