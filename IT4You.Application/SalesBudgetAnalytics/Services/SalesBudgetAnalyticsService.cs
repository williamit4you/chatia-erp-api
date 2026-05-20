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
                    Name = "Visão geral",
                    Description = "Panorama inicial de orçamentos, volume, ticket médio e sazonalidade.",
                    PlannedCount = 15,
                    AvailableNowCount = 15,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "overview_total_amount_period", Title = "Valor total de orçamentos por período", Availability = "available_now" },
                        new() { Id = "overview_total_count_period", Title = "Quantidade de orçamentos por período", Availability = "available_now" },
                        new() { Id = "overview_avg_ticket", Title = "Ticket médio dos orçamentos", Availability = "available_now" },
                        new() { Id = "overview_amount_by_company", Title = "Valor total por empresa/filial", Availability = "available_now" },
                        new() { Id = "overview_count_by_company", Title = "Quantidade de orçamentos por empresa/filial", Availability = "available_now" },
                        new() { Id = "overview_monthly_evolution", Title = "Evolução mensal de orçamentos", Availability = "available_now" },
                        new() { Id = "overview_weekly_evolution", Title = "Evolução semanal de orçamentos", Availability = "available_now" },
                        new() { Id = "overview_daily_evolution", Title = "Evolução diária de orçamentos", Availability = "available_now" },
                        new() { Id = "overview_current_vs_previous_month", Title = "Comparativo mês atual x mês anterior", Availability = "available_now" },
                        new() { Id = "overview_current_year_vs_previous_year", Title = "Comparativo ano atual x ano anterior", Availability = "available_now" },
                        new() { Id = "overview_top_days_by_volume", Title = "Top dias com maior volume de orçamentos", Availability = "available_now" },
                        new() { Id = "overview_top_months_by_amount", Title = "Top meses com maior valor orçado", Availability = "available_now" },
                        new() { Id = "overview_month_seasonality", Title = "Sazonalidade de vendas/orçamentos por mês", Availability = "available_now" },
                        new() { Id = "overview_weekday_heatmap", Title = "Mapa de calor por dia da semana", Availability = "available_now" },
                        new() { Id = "overview_month_year_heatmap", Title = "Mapa de calor por mês e ano", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "funnel",
                    Name = "Funil comercial",
                    Description = "Status, conversão, perdas e gargalos do funil de orçamentos.",
                    PlannedCount = 15,
                    AvailableNowCount = 7,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "funnel_by_status", Title = "Funil por status do orçamento", Availability = "available_now" },
                        new() { Id = "funnel_amount_by_status", Title = "Valor total por status", Availability = "available_now" },
                        new() { Id = "funnel_count_by_status", Title = "Quantidade de orçamentos por status", Availability = "available_now" },
                        new() { Id = "funnel_conversion_percent_by_status", Title = "Percentual de conversão por status", Availability = "available_now" },
                        new() { Id = "funnel_open_approved_lost", Title = "Orçamentos em aberto x aprovados x perdidos", Availability = "available_now" },
                        new() { Id = "funnel_pending_amount", Title = "Valor parado em orçamentos pendentes", Availability = "available_now" },
                        new() { Id = "funnel_approval_rate", Title = "Taxa de aprovação de orçamentos", Availability = "needs_mapping" },
                        new() { Id = "funnel_loss_cancel_rate", Title = "Taxa de perda/cancelamento", Availability = "needs_mapping" },
                        new() { Id = "funnel_conversion_evolution", Title = "Evolução da conversão ao longo do tempo", Availability = "needs_mapping" },
                        new() { Id = "funnel_conversion_by_seller", Title = "Conversão por vendedor", Availability = "needs_mapping" },
                        new() { Id = "funnel_conversion_by_customer", Title = "Conversão por cliente", Availability = "needs_mapping" },
                        new() { Id = "funnel_conversion_by_origin", Title = "Conversão por origem", Availability = "needs_mapping" },
                        new() { Id = "funnel_conversion_by_geo", Title = "Conversão por cidade/UF", Availability = "needs_mapping" },
                        new() { Id = "funnel_conversion_by_payment", Title = "Conversão por condição de pagamento", Availability = "needs_mapping" },
                        new() { Id = "funnel_blocking_status_ranking", Title = "Ranking de status que mais travam vendas", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "seller",
                    Name = "Vendedores",
                    Description = "Performance comercial, ranking, markup, desconto e evolução por vendedor.",
                    PlannedCount = 20,
                    AvailableNowCount = 17,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "seller_total_amount", Title = "Valor total orçado por vendedor", Availability = "available_now" },
                        new() { Id = "seller_total_count", Title = "Quantidade de orçamentos por vendedor", Availability = "available_now" },
                        new() { Id = "seller_avg_ticket", Title = "Ticket médio por vendedor", Availability = "available_now" },
                        new() { Id = "seller_conversion", Title = "Conversão por vendedor", Availability = "needs_mapping" },
                        new() { Id = "seller_avg_discount", Title = "Desconto médio concedido por vendedor", Availability = "available_now" },
                        new() { Id = "seller_avg_markup", Title = "Markup médio por vendedor", Availability = "available_now" },
                        new() { Id = "seller_avg_surcharge", Title = "Acréscimo médio por vendedor", Availability = "available_now" },
                        new() { Id = "seller_avg_freight", Title = "Valor de frete médio por vendedor", Availability = "available_now" },
                        new() { Id = "seller_ranking_amount", Title = "Ranking de vendedores por valor total", Availability = "available_now" },
                        new() { Id = "seller_ranking_count", Title = "Ranking de vendedores por quantidade", Availability = "available_now" },
                        new() { Id = "seller_ranking_ticket", Title = "Ranking de vendedores por ticket médio", Availability = "available_now" },
                        new() { Id = "seller_ranking_markup", Title = "Ranking de vendedores por margem/markup", Availability = "available_now" },
                        new() { Id = "seller_most_lost", Title = "Vendedores com mais orçamentos perdidos", Availability = "needs_mapping" },
                        new() { Id = "seller_most_approved", Title = "Vendedores com mais orçamentos aprovados", Availability = "needs_mapping" },
                        new() { Id = "seller_monthly_evolution", Title = "Evolução mensal por vendedor", Availability = "available_now" },
                        new() { Id = "seller_comparison", Title = "Comparativo entre vendedores", Availability = "available_now" },
                        new() { Id = "seller_share_total", Title = "Participação de cada vendedor no faturamento", Availability = "available_now" },
                        new() { Id = "seller_abc_curve", Title = "Curva ABC de vendedores", Availability = "available_now" },
                        new() { Id = "seller_top_product", Title = "Vendedor x produto mais orçado", Availability = "available_now" },
                        new() { Id = "seller_top_customer", Title = "Vendedor x cliente mais atendido", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "customer",
                    Name = "Clientes",
                    Description = "Ticket, recorrência, conversão, origem e distribuição geográfica dos clientes.",
                    PlannedCount = 20,
                    AvailableNowCount = 18,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "customer_top_amount", Title = "Top clientes por valor orçado", Availability = "available_now" },
                        new() { Id = "customer_top_count", Title = "Top clientes por quantidade de orçamentos", Availability = "available_now" },
                        new() { Id = "customer_avg_ticket", Title = "Ticket médio por cliente", Availability = "available_now" },
                        new() { Id = "customer_recurring", Title = "Clientes recorrentes", Availability = "available_now" },
                        new() { Id = "customer_new_period", Title = "Clientes novos por período", Availability = "available_now" },
                        new() { Id = "customer_inactive_recent", Title = "Clientes sem orçamento recente", Availability = "available_now" },
                        new() { Id = "customer_highest_discount", Title = "Clientes com maior desconto recebido", Availability = "available_now" },
                        new() { Id = "customer_highest_markup", Title = "Clientes com maior markup", Availability = "available_now" },
                        new() { Id = "customer_highest_open_amount", Title = "Clientes com maior valor em aberto", Availability = "available_now" },
                        new() { Id = "customer_highest_conversion", Title = "Clientes com maior taxa de conversão", Availability = "needs_mapping" },
                        new() { Id = "customer_low_conversion", Title = "Clientes com baixa conversão", Availability = "needs_mapping" },
                        new() { Id = "customer_abc_curve", Title = "Curva ABC de clientes", Availability = "available_now" },
                        new() { Id = "customer_top_share", Title = "Participação dos principais clientes no total", Availability = "available_now" },
                        new() { Id = "customer_evolution", Title = "Evolução de orçamentos por cliente", Availability = "available_now" },
                        new() { Id = "customer_top_products", Title = "Cliente x produtos mais orçados", Availability = "available_now" },
                        new() { Id = "customer_responsible_seller", Title = "Cliente x vendedor responsável", Availability = "available_now" },
                        new() { Id = "customer_origin", Title = "Cliente x origem", Availability = "available_now" },
                        new() { Id = "customer_payment_condition", Title = "Cliente x condição de pagamento", Availability = "available_now" },
                        new() { Id = "customer_by_city", Title = "Clientes por cidade", Availability = "available_now" },
                        new() { Id = "customer_by_uf", Title = "Clientes por UF", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "product",
                    Name = "Produtos",
                    Description = "Itens orçados, demanda, valor unitário, mix e associação entre produtos.",
                    PlannedCount = 25,
                    AvailableNowCount = 25,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "product_top_amount", Title = "Top produtos por valor total", Availability = "available_now" },
                        new() { Id = "product_top_quantity", Title = "Top produtos por quantidade orçada", Availability = "available_now" },
                        new() { Id = "product_highest_avg_ticket", Title = "Produtos com maior ticket médio", Availability = "available_now" },
                        new() { Id = "product_highest_discount", Title = "Produtos com maior desconto aplicado", Availability = "available_now" },
                        new() { Id = "product_highest_markup", Title = "Produtos com maior markup", Availability = "available_now" },
                        new() { Id = "product_highest_surcharge", Title = "Produtos com maior acréscimo", Availability = "available_now" },
                        new() { Id = "product_most_quoted_period", Title = "Produtos mais orçados por período", Availability = "available_now" },
                        new() { Id = "product_least_quoted", Title = "Produtos menos orçados", Availability = "available_now" },
                        new() { Id = "product_demand_drop", Title = "Produtos com queda de demanda", Availability = "available_now" },
                        new() { Id = "product_demand_growth", Title = "Produtos com crescimento de demanda", Availability = "available_now" },
                        new() { Id = "product_monthly_evolution", Title = "Evolução mensal por produto", Availability = "available_now" },
                        new() { Id = "product_abc_curve", Title = "Curva ABC de produtos", Availability = "available_now" },
                        new() { Id = "product_share_total", Title = "Participação dos produtos no valor total", Availability = "available_now" },
                        new() { Id = "product_by_seller", Title = "Produtos por vendedor", Availability = "available_now" },
                        new() { Id = "product_by_customer", Title = "Produtos por cliente", Availability = "available_now" },
                        new() { Id = "product_by_geo", Title = "Produtos por cidade/UF", Availability = "available_now" },
                        new() { Id = "product_by_company", Title = "Produtos por empresa/filial", Availability = "available_now" },
                        new() { Id = "product_by_origin", Title = "Produtos por origem do orçamento", Availability = "available_now" },
                        new() { Id = "product_highest_gross_unit", Title = "Produtos com maior valor unitário bruto", Availability = "available_now" },
                        new() { Id = "product_highest_net_unit", Title = "Produtos com maior valor unitário líquido", Availability = "available_now" },
                        new() { Id = "product_gross_net_gap", Title = "Diferença entre valor bruto e líquido", Availability = "available_now" },
                        new() { Id = "product_avg_quantity_per_item", Title = "Quantidade média por item", Availability = "available_now" },
                        new() { Id = "product_avg_value_per_item", Title = "Valor médio por item", Availability = "available_now" },
                        new() { Id = "product_mix_per_budget", Title = "Mix de produtos por orçamento", Availability = "available_now" },
                        new() { Id = "product_cooccurrence", Title = "Produtos que mais aparecem juntos", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "margin",
                    Name = "Descontos e margem",
                    Description = "Desconto, acréscimo, markup e orçamentos com possível margem ruim.",
                    PlannedCount = 25,
                    AvailableNowCount = 24,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "margin_total_discount", Title = "Valor total de descontos concedidos", Availability = "available_now" },
                        new() { Id = "margin_avg_discount_percent", Title = "Percentual médio de desconto", Availability = "available_now" },
                        new() { Id = "margin_discount_by_seller", Title = "Desconto por vendedor", Availability = "available_now" },
                        new() { Id = "margin_discount_by_customer", Title = "Desconto por cliente", Availability = "available_now" },
                        new() { Id = "margin_discount_by_product", Title = "Desconto por produto", Availability = "available_now" },
                        new() { Id = "margin_discount_by_origin", Title = "Desconto por origem", Availability = "available_now" },
                        new() { Id = "margin_discount_by_payment", Title = "Desconto por condição de pagamento", Availability = "available_now" },
                        new() { Id = "margin_highest_discount_ranking", Title = "Ranking de maiores descontos", Availability = "available_now" },
                        new() { Id = "margin_above_avg_discount_budgets", Title = "Orçamentos com desconto acima da média", Availability = "available_now" },
                        new() { Id = "margin_discount_impact_total", Title = "Impacto do desconto no valor total", Availability = "available_now" },
                        new() { Id = "margin_discount_vs_conversion", Title = "Relação desconto x conversão", Availability = "needs_mapping" },
                        new() { Id = "margin_discount_vs_seller", Title = "Relação desconto x vendedor", Availability = "available_now" },
                        new() { Id = "margin_total_surcharge", Title = "Valor total de acréscimos", Availability = "available_now" },
                        new() { Id = "margin_avg_surcharge_percent", Title = "Percentual médio de acréscimo", Availability = "available_now" },
                        new() { Id = "margin_surcharge_by_seller", Title = "Acréscimo por vendedor", Availability = "available_now" },
                        new() { Id = "margin_surcharge_by_customer", Title = "Acréscimo por cliente", Availability = "available_now" },
                        new() { Id = "margin_surcharge_by_product", Title = "Acréscimo por produto", Availability = "available_now" },
                        new() { Id = "margin_avg_markup_general", Title = "Markup médio geral", Availability = "available_now" },
                        new() { Id = "margin_markup_by_seller", Title = "Markup por vendedor", Availability = "available_now" },
                        new() { Id = "margin_markup_by_product", Title = "Markup por produto", Availability = "available_now" },
                        new() { Id = "margin_markup_by_customer", Title = "Markup por cliente", Availability = "available_now" },
                        new() { Id = "margin_markup_by_origin", Title = "Markup por origem", Availability = "available_now" },
                        new() { Id = "margin_low_markup_budgets", Title = "Orçamentos com markup baixo", Availability = "available_now" },
                        new() { Id = "margin_possible_bad_margin_budgets", Title = "Orçamentos com possível margem ruim", Availability = "available_now" },
                        new() { Id = "margin_gross_vs_net", Title = "Comparativo valor bruto x valor líquido", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "source",
                    Name = "Origem",
                    Description = "Canais de venda, participação, ticket e conversão por origem.",
                    PlannedCount = 15,
                    AvailableNowCount = 13,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "source_total_amount", Title = "Valor total por origem", Availability = "available_now" },
                        new() { Id = "source_total_count", Title = "Quantidade de orçamentos por origem", Availability = "available_now" },
                        new() { Id = "source_avg_ticket", Title = "Ticket médio por origem", Availability = "available_now" },
                        new() { Id = "source_conversion", Title = "Conversão por origem", Availability = "needs_mapping" },
                        new() { Id = "source_highest_avg_discount", Title = "Origem com maior desconto médio", Availability = "available_now" },
                        new() { Id = "source_highest_markup", Title = "Origem com maior markup", Availability = "available_now" },
                        new() { Id = "source_evolution", Title = "Evolução de origens por período", Availability = "available_now" },
                        new() { Id = "source_share_total", Title = "Participação de cada origem no total", Availability = "available_now" },
                        new() { Id = "source_by_seller", Title = "Origem x vendedor", Availability = "available_now" },
                        new() { Id = "source_by_product", Title = "Origem x produto", Availability = "available_now" },
                        new() { Id = "source_by_customer", Title = "Origem x cliente", Availability = "available_now" },
                        new() { Id = "source_by_geo", Title = "Origem x cidade/UF", Availability = "available_now" },
                        new() { Id = "source_best_channels", Title = "Ranking de melhores canais de venda", Availability = "available_now" },
                        new() { Id = "source_high_volume_low_conversion", Title = "Canais com muito orçamento e pouca conversão", Availability = "needs_mapping" },
                        new() { Id = "source_low_volume_high_ticket", Title = "Canais com menos volume, mas maior ticket", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "geo",
                    Name = "Geografia",
                    Description = "Distribuição por UF, cidade, região, vendedor e oportunidade geográfica.",
                    PlannedCount = 17,
                    AvailableNowCount = 16,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "geo_amount_by_uf", Title = "Valor total por UF", Availability = "available_now" },
                        new() { Id = "geo_count_by_uf", Title = "Quantidade de orçamentos por UF", Availability = "available_now" },
                        new() { Id = "geo_avg_ticket_by_uf", Title = "Ticket médio por UF", Availability = "available_now" },
                        new() { Id = "geo_conversion_by_uf", Title = "Conversão por UF", Availability = "needs_mapping" },
                        new() { Id = "geo_amount_by_city", Title = "Valor total por cidade", Availability = "available_now" },
                        new() { Id = "geo_count_by_city", Title = "Quantidade por cidade", Availability = "available_now" },
                        new() { Id = "geo_top_cities_count", Title = "Ranking de cidades com mais orçamentos", Availability = "available_now" },
                        new() { Id = "geo_top_cities_ticket", Title = "Ranking de cidades com maior ticket médio", Availability = "available_now" },
                        new() { Id = "geo_state_heatmap", Title = "Mapa de calor por estado", Availability = "available_now" },
                        new() { Id = "geo_city_heatmap", Title = "Mapa de calor por cidade", Availability = "available_now" },
                        new() { Id = "geo_seller_by_region", Title = "Vendedor por região", Availability = "available_now" },
                        new() { Id = "geo_top_product_by_uf", Title = "Produto mais orçado por UF", Availability = "available_now" },
                        new() { Id = "geo_customer_by_region", Title = "Cliente por região", Availability = "available_now" },
                        new() { Id = "geo_origin_by_region", Title = "Origem por região", Availability = "available_now" },
                        new() { Id = "geo_highest_avg_discount_regions", Title = "Regiões com maior desconto médio", Availability = "available_now" },
                        new() { Id = "geo_highest_markup_regions", Title = "Regiões com maior markup", Availability = "available_now" },
                        new() { Id = "geo_growth_opportunity_regions", Title = "Regiões com maior oportunidade de crescimento", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "payment",
                    Name = "Condição de pagamento",
                    Description = "Uso, ticket, desconto, markup e aprovação por condição de pagamento.",
                    PlannedCount = 12,
                    AvailableNowCount = 10,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "payment_total_amount", Title = "Valor total por condição de pagamento", Availability = "available_now" },
                        new() { Id = "payment_total_count", Title = "Quantidade por condição de pagamento", Availability = "available_now" },
                        new() { Id = "payment_avg_ticket", Title = "Ticket médio por condição de pagamento", Availability = "available_now" },
                        new() { Id = "payment_conversion", Title = "Conversão por condição de pagamento", Availability = "needs_mapping" },
                        new() { Id = "payment_avg_discount", Title = "Desconto médio por condição de pagamento", Availability = "available_now" },
                        new() { Id = "payment_avg_markup", Title = "Markup médio por condição de pagamento", Availability = "available_now" },
                        new() { Id = "payment_most_used", Title = "Condições de pagamento mais usadas", Availability = "available_now" },
                        new() { Id = "payment_by_seller", Title = "Condição de pagamento x vendedor", Availability = "available_now" },
                        new() { Id = "payment_by_customer", Title = "Condição de pagamento x cliente", Availability = "available_now" },
                        new() { Id = "payment_by_product", Title = "Condição de pagamento x produto", Availability = "available_now" },
                        new() { Id = "payment_by_origin", Title = "Condição de pagamento x origem", Availability = "available_now" },
                        new() { Id = "payment_vs_approval", Title = "Condição de pagamento x aprovação", Availability = "needs_mapping" },
                    }
                },
                new()
                {
                    Id = "freight",
                    Name = "Frete",
                    Description = "Valor de frete, tipo, impacto no ticket e relação com conversão.",
                    PlannedCount = 11,
                    AvailableNowCount = 10,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "freight_total_amount", Title = "Valor total de frete", Availability = "available_now" },
                        new() { Id = "freight_avg_per_budget", Title = "Frete médio por orçamento", Availability = "available_now" },
                        new() { Id = "freight_by_seller", Title = "Frete por vendedor", Availability = "available_now" },
                        new() { Id = "freight_by_customer", Title = "Frete por cliente", Availability = "available_now" },
                        new() { Id = "freight_by_geo", Title = "Frete por cidade/UF", Availability = "available_now" },
                        new() { Id = "freight_by_type", Title = "Frete por tipo de frete", Availability = "available_now" },
                        new() { Id = "freight_ratio_total", Title = "Frete em relação ao valor total", Availability = "available_now" },
                        new() { Id = "freight_high_budgets", Title = "Orçamentos com frete alto", Availability = "available_now" },
                        new() { Id = "freight_vs_conversion", Title = "Relação frete x conversão", Availability = "needs_mapping" },
                        new() { Id = "freight_most_used_type", Title = "Tipo de frete mais usado", Availability = "available_now" },
                        new() { Id = "freight_avg_ticket_by_type", Title = "Ticket médio por tipo de frete", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "executive",
                    Name = "Diretoria",
                    Description = "Indicadores consolidados, pipeline, alertas e leitura executiva de vendas.",
                    PlannedCount = 20,
                    AvailableNowCount = 18,
                    NeedsNewViewCount = 2,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "exec_dashboard", Title = "Dashboard executivo de vendas", Availability = "available_now" },
                        new() { Id = "exec_total_revenue_budget", Title = "Receita/orçamento total por período", Availability = "available_now" },
                        new() { Id = "exec_goal_vs_actual", Title = "Meta x realizado", Availability = "needs_new_view" },
                        new() { Id = "exec_sales_forecast", Title = "Forecast de vendas", Availability = "needs_new_view" },
                        new() { Id = "exec_open_pipeline", Title = "Pipeline comercial em aberto", Availability = "available_now" },
                        new() { Id = "exec_opportunity_ranking", Title = "Ranking de oportunidades", Availability = "available_now" },
                        new() { Id = "exec_monthly_growth", Title = "Evolução de crescimento mensal", Availability = "available_now" },
                        new() { Id = "exec_seller_share", Title = "Participação por vendedor", Availability = "available_now" },
                        new() { Id = "exec_customer_share", Title = "Participação por cliente", Availability = "available_now" },
                        new() { Id = "exec_product_share", Title = "Participação por produto", Availability = "available_now" },
                        new() { Id = "exec_region_share", Title = "Participação por região", Availability = "available_now" },
                        new() { Id = "exec_origin_share", Title = "Participação por origem", Availability = "available_now" },
                        new() { Id = "exec_avg_markup", Title = "Margem/markup médio consolidado", Availability = "available_now" },
                        new() { Id = "exec_total_discount", Title = "Desconto total concedido", Availability = "available_now" },
                        new() { Id = "exec_lost_opportunities", Title = "Oportunidades perdidas", Availability = "available_now" },
                        new() { Id = "exec_negotiation_opportunities", Title = "Oportunidades em negociação", Availability = "available_now" },
                        new() { Id = "exec_strategic_customers", Title = "Clientes estratégicos", Availability = "available_now" },
                        new() { Id = "exec_strategic_products", Title = "Produtos estratégicos", Availability = "available_now" },
                        new() { Id = "exec_strategic_channels", Title = "Canais estratégicos", Availability = "available_now" },
                        new() { Id = "exec_sales_drop_alerts", Title = "Alertas de queda de vendas", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "seller_insights",
                    Name = "Insights para vendedores",
                    Description = "Follow-up, potencial de recompra, comparativos e oportunidades individuais.",
                    PlannedCount = 20,
                    AvailableNowCount = 20,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "insight_customers_high_conversion_chance", Title = "Clientes com maior chance de conversão", Availability = "available_now" },
                        new() { Id = "insight_frequent_customers", Title = "Clientes que orçam com frequência", Availability = "available_now" },
                        new() { Id = "insight_customers_stopped_quoting", Title = "Clientes que pararam de orçar", Availability = "available_now" },
                        new() { Id = "insight_recommended_products_by_customer", Title = "Produtos mais indicados por cliente", Availability = "available_now" },
                        new() { Id = "insight_top_products_by_region", Title = "Produtos mais vendidos por região", Availability = "available_now" },
                        new() { Id = "insight_high_acceptance_products", Title = "Produtos com maior aceitação", Availability = "available_now" },
                        new() { Id = "insight_pending_followup_budgets", Title = "Orçamentos pendentes de follow-up", Availability = "available_now" },
                        new() { Id = "insight_old_open_budgets", Title = "Orçamentos antigos ainda em aberto", Availability = "available_now" },
                        new() { Id = "insight_high_value_no_return", Title = "Orçamentos de alto valor sem retorno", Availability = "available_now" },
                        new() { Id = "insight_high_ticket_customers", Title = "Clientes com alto ticket médio", Availability = "available_now" },
                        new() { Id = "insight_discount_sensitive_customers", Title = "Clientes sensíveis a desconto", Availability = "available_now" },
                        new() { Id = "insight_low_discount_customers", Title = "Clientes que compram sem muito desconto", Availability = "available_now" },
                        new() { Id = "insight_best_origin_by_seller", Title = "Melhor origem para cada vendedor", Availability = "available_now" },
                        new() { Id = "insight_best_product_by_seller", Title = "Melhor produto para cada vendedor", Availability = "available_now" },
                        new() { Id = "insight_best_region_by_seller", Title = "Melhor região para cada vendedor", Availability = "available_now" },
                        new() { Id = "insight_seller_vs_team_avg", Title = "Comparativo do vendedor com a equipe", Availability = "available_now" },
                        new() { Id = "insight_personal_seller_ranking", Title = "Ranking pessoal do vendedor", Availability = "available_now" },
                        new() { Id = "insight_individual_monthly_evolution", Title = "Evolução mensal individual", Availability = "available_now" },
                        new() { Id = "insight_underused_products_by_seller", Title = "Produtos que o vendedor vende pouco", Availability = "available_now" },
                        new() { Id = "insight_repurchase_potential_customers", Title = "Clientes com potencial de recompra", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "future_data",
                    Name = "Novas informações",
                    Description = "Gráficos dependentes de pedido, faturamento, estoque, custo, metas e marketing.",
                    PlannedCount = 30,
                    AvailableNowCount = 0,
                    NeedsNewViewCount = 30,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "future_budget_vs_sold", Title = "Orçado x vendido/faturado", Availability = "needs_new_view" },
                        new() { Id = "future_budget_converted_to_order", Title = "Orçamento convertido em pedido", Availability = "needs_new_view" },
                        new() { Id = "future_avg_conversion_time", Title = "Tempo médio de conversão", Availability = "needs_new_view" },
                        new() { Id = "future_issue_to_approval_time", Title = "Tempo médio entre emissão e aprovação", Availability = "needs_new_view" },
                        new() { Id = "future_loss_reason", Title = "Motivo de perda do orçamento", Availability = "needs_new_view" },
                        new() { Id = "future_cancel_reason", Title = "Motivo de cancelamento", Availability = "needs_new_view" },
                        new() { Id = "future_goal_by_seller", Title = "Meta por vendedor", Availability = "needs_new_view" },
                        new() { Id = "future_goal_by_company", Title = "Meta por filial", Availability = "needs_new_view" },
                        new() { Id = "future_goal_by_product", Title = "Meta por produto", Availability = "needs_new_view" },
                        new() { Id = "future_goal_by_region", Title = "Meta por região", Availability = "needs_new_view" },
                        new() { Id = "future_real_margin_by_product", Title = "Margem real por produto", Availability = "needs_new_view" },
                        new() { Id = "future_gross_profit", Title = "Lucro bruto", Availability = "needs_new_view" },
                        new() { Id = "future_cogs", Title = "Custo do produto vendido", Availability = "needs_new_view" },
                        new() { Id = "future_available_stock", Title = "Estoque disponível", Availability = "needs_new_view" },
                        new() { Id = "future_stockout", Title = "Ruptura de estoque", Availability = "needs_new_view" },
                        new() { Id = "future_no_stock_lost_sales", Title = "Produtos sem estoque que perderam venda", Availability = "needs_new_view" },
                        new() { Id = "future_inventory_turnover", Title = "Giro de estoque", Availability = "needs_new_view" },
                        new() { Id = "future_demand_forecast", Title = "Previsão de demanda", Availability = "needs_new_view" },
                        new() { Id = "future_seller_commission", Title = "Comissão por vendedor", Availability = "needs_new_view" },
                        new() { Id = "future_profitability_by_seller", Title = "Rentabilidade por vendedor", Availability = "needs_new_view" },
                        new() { Id = "future_profitability_by_customer", Title = "Rentabilidade por cliente", Availability = "needs_new_view" },
                        new() { Id = "future_profitability_by_product", Title = "Rentabilidade por produto", Availability = "needs_new_view" },
                        new() { Id = "future_customer_default", Title = "Inadimplência por cliente", Availability = "needs_new_view" },
                        new() { Id = "future_avg_payment_term", Title = "Prazo médio de pagamento", Availability = "needs_new_view" },
                        new() { Id = "future_customer_ltv", Title = "Lifetime Value do cliente", Availability = "needs_new_view" },
                        new() { Id = "future_customer_churn", Title = "Churn de clientes", Availability = "needs_new_view" },
                        new() { Id = "future_customer_repurchase", Title = "Recompra por cliente", Availability = "needs_new_view" },
                        new() { Id = "future_avg_purchase_frequency", Title = "Frequência média de compra", Availability = "needs_new_view" },
                        new() { Id = "future_real_cross_sell_upsell", Title = "Cross-sell e up-sell real", Availability = "needs_new_view" },
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
                        new() { Id = "kpi_total_budget_amount", Title = "Valor total orçado", Availability = "available_now" },
                        new() { Id = "kpi_budget_count", Title = "Quantidade de orçamentos", Availability = "available_now" },
                        new() { Id = "kpi_avg_ticket", Title = "Ticket médio", Availability = "available_now" },
                        new() { Id = "kpi_conversion_rate", Title = "Taxa de conversão", Availability = "needs_mapping" },
                        new() { Id = "kpi_open_amount", Title = "Valor em aberto", Availability = "available_now" },
                        new() { Id = "kpi_approved_amount", Title = "Valor aprovado", Availability = "available_now" },
                        new() { Id = "kpi_lost_amount", Title = "Valor perdido", Availability = "available_now" },
                        new() { Id = "kpi_best_seller", Title = "Melhor vendedor", Availability = "available_now" },
                        new() { Id = "kpi_best_customer", Title = "Melhor cliente", Availability = "available_now" },
                        new() { Id = "kpi_best_product", Title = "Melhor produto", Availability = "available_now" },
                        new() { Id = "kpi_best_city", Title = "Melhor cidade", Availability = "available_now" },
                        new() { Id = "kpi_best_uf", Title = "Melhor UF", Availability = "available_now" },
                        new() { Id = "kpi_best_origin", Title = "Melhor origem", Availability = "available_now" },
                        new() { Id = "kpi_highest_discount", Title = "Maior desconto concedido", Availability = "available_now" },
                        new() { Id = "kpi_avg_discount", Title = "Desconto médio", Availability = "available_now" },
                        new() { Id = "kpi_avg_markup", Title = "Markup médio", Availability = "available_now" },
                        new() { Id = "kpi_avg_freight", Title = "Frete médio", Availability = "available_now" },
                        new() { Id = "kpi_most_quoted_product", Title = "Produto mais orçado", Availability = "available_now" },
                        new() { Id = "kpi_highest_potential_customer", Title = "Cliente com maior potencial", Availability = "available_now" },
                        new() { Id = "kpi_seller_highest_growth", Title = "Vendedor com maior crescimento", Availability = "available_now" },
                        new() { Id = "kpi_seller_highest_drop", Title = "Vendedor com maior queda", Availability = "available_now" },
                        new() { Id = "kpi_product_highest_growth", Title = "Produto com maior crescimento", Availability = "available_now" },
                        new() { Id = "kpi_product_highest_drop", Title = "Produto com maior queda", Availability = "available_now" },
                        new() { Id = "kpi_channel_highest_conversion", Title = "Canal com maior conversão", Availability = "needs_mapping" },
                        new() { Id = "kpi_channel_lowest_conversion", Title = "Canal com menor conversão", Availability = "needs_mapping" },
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
