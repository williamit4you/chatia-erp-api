using System.Collections.Generic;
using System.Threading.Tasks;
using IT4You.Application.SalesBudgetAnalytics.DTOs;
using IT4You.Application.SalesBudgetAnalytics.Interfaces;

namespace IT4You.Application.SalesBudgetAnalytics.Services;

public class SalesBudgetAnalyticsService : ISalesBudgetAnalyticsService
{
    private readonly ISalesBudgetAnalyticsRepository _repository;

    public SalesBudgetAnalyticsService(ISalesBudgetAnalyticsRepository repository)
    {
        _repository = repository;
    }

    public Task<SalesBudgetCatalogResponseDto> GetCatalogAsync()
    {
        return Task.FromResult(new SalesBudgetCatalogResponseDto
        {
            Categories = new List<SalesBudgetCategoryDto>
            {
                new()
                {
                    Id = "overview",
                    Name = "Visao geral",
                    Description = "Panorama inicial de orcamentos, volume, ticket medio e sazonalidade.",
                    PlannedCount = 15,
                    AvailableNowCount = 15,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "overview_total_amount_period", Title = "Valor total de orcamentos por periodo", Availability = "available_now" },
                        new() { Id = "overview_total_count_period", Title = "Quantidade de orcamentos por periodo", Availability = "available_now" },
                        new() { Id = "overview_monthly_evolution", Title = "Evolucao mensal de orcamentos", Availability = "available_now" },
                        new() { Id = "overview_weekday_heatmap", Title = "Mapa de calor por dia da semana", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "funnel",
                    Name = "Funil comercial",
                    Description = "Status, conversao, perdas e gargalos do funil de orcamentos.",
                    PlannedCount = 15,
                    AvailableNowCount = 12,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "funnel_by_status", Title = "Funil por status do orcamento", Availability = "available_now" },
                        new() { Id = "funnel_approval_rate", Title = "Taxa de aprovacao de orcamentos", Availability = "needs_mapping" },
                        new() { Id = "funnel_conversion_by_seller", Title = "Conversao por vendedor", Availability = "needs_mapping" },
                        new() { Id = "funnel_blocking_status_ranking", Title = "Ranking de status que mais travam vendas", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "seller",
                    Name = "Vendedores",
                    Description = "Performance comercial, ranking, markup, desconto e evolucao por vendedor.",
                    PlannedCount = 20,
                    AvailableNowCount = 18,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "seller_total_amount", Title = "Valor total orcado por vendedor", Availability = "available_now" },
                        new() { Id = "seller_conversion", Title = "Conversao por vendedor", Availability = "needs_mapping" },
                        new() { Id = "seller_abc_curve", Title = "Curva ABC de vendedores", Availability = "available_now" },
                        new() { Id = "seller_top_product", Title = "Vendedor x produto mais orcado", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "customer",
                    Name = "Clientes",
                    Description = "Ticket, recorrencia, conversao, origem e distribuicao geografica dos clientes.",
                    PlannedCount = 20,
                    AvailableNowCount = 17,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "customer_top_amount", Title = "Top clientes por valor orcado", Availability = "available_now" },
                        new() { Id = "customer_recurring", Title = "Clientes recorrentes", Availability = "available_now" },
                        new() { Id = "customer_highest_conversion", Title = "Clientes com maior taxa de conversao", Availability = "needs_mapping" },
                        new() { Id = "customer_top_products", Title = "Cliente x produtos mais orcados", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "product",
                    Name = "Produtos",
                    Description = "Itens orcados, demanda, valor unitario, mix e associacao entre produtos.",
                    PlannedCount = 25,
                    AvailableNowCount = 23,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "product_top_amount", Title = "Top produtos por valor total", Availability = "available_now" },
                        new() { Id = "product_demand_growth", Title = "Produtos com crescimento de demanda", Availability = "available_now" },
                        new() { Id = "product_mix_per_budget", Title = "Mix de produtos por orcamento", Availability = "available_now" },
                        new() { Id = "product_cooccurrence", Title = "Produtos que mais aparecem juntos", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "margin",
                    Name = "Descontos e margem",
                    Description = "Desconto, acrescimo, markup e orcamentos com possivel margem ruim.",
                    PlannedCount = 25,
                    AvailableNowCount = 22,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "margin_total_discount", Title = "Valor total de descontos concedidos", Availability = "available_now" },
                        new() { Id = "margin_discount_vs_conversion", Title = "Relacao desconto x conversao", Availability = "needs_mapping" },
                        new() { Id = "margin_avg_markup_general", Title = "Markup medio geral", Availability = "available_now" },
                        new() { Id = "margin_possible_bad_margin_budgets", Title = "Orcamentos com possivel margem ruim", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "source",
                    Name = "Origem",
                    Description = "Canais de venda, participacao, ticket e conversao por origem.",
                    PlannedCount = 15,
                    AvailableNowCount = 13,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "source_total_amount", Title = "Valor total por origem", Availability = "available_now" },
                        new() { Id = "source_conversion", Title = "Conversao por origem", Availability = "needs_mapping" },
                        new() { Id = "source_best_channels", Title = "Ranking de melhores canais de venda", Availability = "available_now" },
                        new() { Id = "source_high_volume_low_conversion", Title = "Muito orcamento e pouca conversao", Availability = "needs_mapping" },
                    }
                },
                new()
                {
                    Id = "geo",
                    Name = "Geografia",
                    Description = "Distribuicao por UF, cidade, regiao, vendedor e oportunidade geografica.",
                    PlannedCount = 17,
                    AvailableNowCount = 15,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "geo_amount_by_uf", Title = "Valor total por UF", Availability = "available_now" },
                        new() { Id = "geo_count_by_city", Title = "Quantidade por cidade", Availability = "available_now" },
                        new() { Id = "geo_state_heatmap", Title = "Mapa de calor por estado", Availability = "available_now" },
                        new() { Id = "geo_growth_opportunity_regions", Title = "Regioes com oportunidade de crescimento", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "payment",
                    Name = "Condicao de pagamento",
                    Description = "Uso, ticket, desconto, markup e aprovacao por condicao de pagamento.",
                    PlannedCount = 12,
                    AvailableNowCount = 11,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "payment_total_amount", Title = "Valor total por condicao de pagamento", Availability = "available_now" },
                        new() { Id = "payment_conversion", Title = "Conversao por condicao de pagamento", Availability = "needs_mapping" },
                        new() { Id = "payment_most_used", Title = "Condicoes de pagamento mais usadas", Availability = "available_now" },
                        new() { Id = "payment_vs_approval", Title = "Condicao de pagamento x aprovacao", Availability = "needs_mapping" },
                    }
                },
                new()
                {
                    Id = "freight",
                    Name = "Frete",
                    Description = "Valor de frete, tipo, impacto no ticket e relacao com conversao.",
                    PlannedCount = 11,
                    AvailableNowCount = 10,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "freight_total_amount", Title = "Valor total de frete", Availability = "available_now" },
                        new() { Id = "freight_by_type", Title = "Frete por tipo de frete", Availability = "available_now" },
                        new() { Id = "freight_ratio_total", Title = "Frete em relacao ao valor total", Availability = "available_now" },
                        new() { Id = "freight_vs_conversion", Title = "Relacao frete x conversao", Availability = "needs_mapping" },
                    }
                },
                new()
                {
                    Id = "executive",
                    Name = "Diretoria",
                    Description = "Indicadores consolidados, pipeline, alertas e leitura executiva de vendas.",
                    PlannedCount = 20,
                    AvailableNowCount = 15,
                    NeedsNewViewCount = 2,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "exec_dashboard", Title = "Dashboard executivo de vendas", Availability = "available_now" },
                        new() { Id = "exec_open_pipeline", Title = "Pipeline comercial em aberto", Availability = "available_now" },
                        new() { Id = "exec_goal_vs_actual", Title = "Meta x realizado", Availability = "needs_new_view" },
                        new() { Id = "exec_sales_forecast", Title = "Forecast de vendas", Availability = "needs_new_view" },
                    }
                },
                new()
                {
                    Id = "seller_insights",
                    Name = "Insights para vendedores",
                    Description = "Follow-up, potencial de recompra, comparativos e oportunidades individuais.",
                    PlannedCount = 20,
                    AvailableNowCount = 17,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "insight_pending_followup_budgets", Title = "Orcamentos pendentes de follow-up", Availability = "available_now" },
                        new() { Id = "insight_high_value_no_return", Title = "Orcamentos de alto valor sem retorno", Availability = "available_now" },
                        new() { Id = "insight_seller_vs_team_avg", Title = "Comparativo do vendedor com a equipe", Availability = "available_now" },
                        new() { Id = "insight_repurchase_potential_customers", Title = "Clientes com potencial de recompra", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "future_data",
                    Name = "Novas informacoes",
                    Description = "Graficos dependentes de pedido, faturamento, estoque, custo, metas e marketing.",
                    PlannedCount = 30,
                    AvailableNowCount = 0,
                    NeedsNewViewCount = 30,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "future_budget_vs_sold", Title = "Orcado x vendido/faturado", Availability = "needs_new_view" },
                        new() { Id = "future_loss_reason", Title = "Motivo de perda do orcamento", Availability = "needs_new_view" },
                        new() { Id = "future_real_margin_by_product", Title = "Margem real por produto", Availability = "needs_new_view" },
                        new() { Id = "future_marketing_campaign_sales", Title = "Campanha de marketing x venda", Availability = "needs_new_view" },
                    }
                },
                new()
                {
                    Id = "kpis",
                    Name = "KPIs essenciais",
                    Description = "Cards principais para abrir a tela com leitura executiva imediata.",
                    PlannedCount = 25,
                    AvailableNowCount = 22,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "kpi_total_budget_amount", Title = "Valor total orcado", Availability = "available_now" },
                        new() { Id = "kpi_conversion_rate", Title = "Taxa de conversao", Availability = "needs_mapping" },
                        new() { Id = "kpi_best_seller", Title = "Melhor vendedor", Availability = "available_now" },
                        new() { Id = "kpi_channel_highest_conversion", Title = "Canal com maior conversao", Availability = "needs_mapping" },
                    }
                }
            }
        });
    }

    public Task<SalesBudgetKpiResponseDto> GetKpisAsync(SalesBudgetKpiRequestDto request)
    {
        return _repository.GetKpisAsync(request);
    }

    public Task<SalesBudgetChartBatchResponseDto> GetChartsAsync(SalesBudgetChartBatchRequestDto request)
    {
        return _repository.GetChartsAsync(request);
    }
}
