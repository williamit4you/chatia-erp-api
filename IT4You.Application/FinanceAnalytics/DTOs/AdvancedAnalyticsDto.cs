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
        
        public decimal PrazoMedioPagamento { get; set; }
        public decimal PrazoMedioRecebimento { get; set; }
        public decimal TicketMedioPagamento { get; set; }
        public decimal TicketMedioRecebimento { get; set; }
        public decimal IndiceLiquidezOperacional { get; set; }

        // Novos KPIs Fase 2
        public decimal IndiceCobertura { get; set; }
        public decimal GapFinanceiro { get; set; }
        public decimal SaldoFinanceiro30Dias { get; set; }
        public decimal PercRecebidoNoPrazo { get; set; }
        public decimal PercPagoNoPrazo { get; set; }
        public decimal DiasMedioAtrasoReceber { get; set; }
        public decimal DiasMedioAtrasoPagar { get; set; }
        public decimal IndiceDependenciaCliente { get; set; }
        public decimal IndiceDependenciaFornecedor { get; set; }
        public decimal RotacaoFinanceira { get; set; }
        public decimal PrazoMedioRestanteReceber { get; set; }
        public decimal PrazoMedioRestantePagar { get; set; }
        public decimal PercParcelasAtraso { get; set; }
        public decimal ValorMedioCliente { get; set; }
        public decimal ValorMedioFornecedor { get; set; }
        public decimal IndiceLiquidacaoDocumentos { get; set; }
        public decimal ConcentracaoTop5Clientes { get; set; }
        public decimal ConcentracaoTop5Fornecedores { get; set; }
        public decimal CrescimentoRecebimentos { get; set; }
        public decimal CrescimentoPagamentos { get; set; }
        public decimal PercContasProximoVencimento { get; set; }
        public decimal MediaDiasEmissaoVencimento { get; set; }
        public decimal ValorMedioParcelamento { get; set; }

        // Novos KPIs Fase 3
        public decimal PercRecebimentoAntecipado { get; set; }
        public decimal PercPagamentoAntecipado { get; set; }
        public decimal TempoMedioEmissaoPagamento { get; set; }
        public decimal PercDocumentosParcelados { get; set; }
        public decimal MediaParcelasPorDocumento { get; set; }
        public decimal PercRecebimentos7Dias { get; set; }
        public decimal TempoMedioRestanteVencimento { get; set; }
    }

    public class MonthlyEvolutionDto
    {
        public int Ano { get; set; }
        public int Mes { get; set; }
        public decimal Valor { get; set; }
        public string MesAno => $"{Ano:0000}-{Mes:00}";
    }

    public class TopAccountDto
    {
        public string Documento { get; set; }
        public decimal Valor { get; set; }
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

        // Novos Gráficos
        public IEnumerable<DistributionDto> DistribuicaoPagarFornecedor { get; set; }
        public IEnumerable<DistributionDto> DistribuicaoReceberCliente { get; set; }
        public IEnumerable<GeographicDto> GeograficoPagar { get; set; }
        public IEnumerable<GeographicDto> GeograficoReceber { get; set; }
        public IEnumerable<DistributionDto> DistribuicaoTipoPagamento { get; set; }
        public IEnumerable<DistributionDto> DistribuicaoCondicaoPagamento { get; set; }
        public IEnumerable<MonthlyEvolutionDto> EvolucaoMensalPagamento { get; set; }
        public IEnumerable<MonthlyEvolutionDto> EvolucaoMensalRecebimento { get; set; }
        public IEnumerable<MonthlyEvolutionDto> CurvaVencimentoPagar { get; set; }
        public IEnumerable<MonthlyEvolutionDto> CurvaVencimentoReceber { get; set; }
        public IEnumerable<TopAccountDto> TopContasPagar { get; set; }
        public IEnumerable<TopAccountDto> TopContasReceber { get; set; }
        public IEnumerable<DistributionDto> DistribuicaoFaixaValorPagar { get; set; }
        public IEnumerable<DistributionDto> DistribuicaoFaixaValorReceber { get; set; }

        // Novas Coleções Fase 2
        public IEnumerable<DistributionDto> VolumePorDia { get; set; }
        public IEnumerable<DistributionDto> IndiceLiquidezPorEmpresa { get; set; }
        public IEnumerable<MonthlyEvolutionDto> FluxoCaixaDiarioProjetado { get; set; }
        public IEnumerable<DistributionDto> VolumePorCpfCnpj { get; set; }
        public IEnumerable<DistributionDto> DistribuicaoFaixaPrazoVencimento { get; set; }

        // Novas Coleções Fase 3
        public IEnumerable<DistributionDto> PrazoMedioRecebimentoPorCliente { get; set; }
        public IEnumerable<DistributionDto> PrazoMedioPagamentoPorFornecedor { get; set; }
        public IEnumerable<DistributionDto> TicketMedioPorCliente { get; set; }
        public IEnumerable<DistributionDto> TicketMedioPorFornecedor { get; set; }
        public IEnumerable<DistributionDto> DocumentosPorClienteAtivo { get; set; }
        public IEnumerable<DistributionDto> DocumentosPorFornecedorAtivo { get; set; }
    }
}
