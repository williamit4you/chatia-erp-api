using System;
using System.Collections.Generic;

namespace IT4You.Application.FinanceAnalytics.DTOs
{
    public class AgingDto
    {
        public string Faixa { get; set; } // A vencer, 1-30 dias, etc.
        public decimal Valor { get; set; }
    }

    public class GeographicDto
    {
        public string Local { get; set; } // UF or Cidade
        public decimal Valor { get; set; }
    }

    public class DistributionDto
    {
        public string Label { get; set; }
        public decimal Valor { get; set; }
        public decimal Percentual { get; set; }
    }

    public class PerformanceDto
    {
        public string Categoria { get; set; } // No prazo, Com atraso, Não recebido
        public decimal Valor { get; set; }
    }

    public class BalancePointDto
    {
        public DateTime Data { get; set; }
        public decimal Valor { get; set; }
        public decimal SaldoAcumulado { get; set; }
    }

    public class CashProjectionDto
    {
        public DateTime Data { get; set; }
        public decimal SaldoPrevisto { get; set; }
        public decimal Recebimentos { get; set; }
        public decimal Pagamentos { get; set; }
    }

    public class FinancialHealthDto
    {
        public decimal Score { get; set; } // 0-100
        public decimal Inadimplencia { get; set; } // Percentual
        public decimal DSO { get; set; } // Days
        public decimal ConcentracaoReceita { get; set; } // Pareto index or top client %
    }

    public class AdvancedDashboardDto
    {
        public IEnumerable<AgingDto> Aging { get; set; }
        public IEnumerable<GeographicDto> Geografico { get; set; }
        public IEnumerable<DistributionDto> DistribuicaoReceber { get; set; }
        public IEnumerable<PerformanceDto> PerformanceRecebimento { get; set; }
        public IEnumerable<BalancePointDto> EvolucaoSaldo { get; set; }
        public IEnumerable<CashProjectionDto> PrevisaoCaixa { get; set; }
        public FinancialHealthDto SaudeFinanceira { get; set; }
    }
}
