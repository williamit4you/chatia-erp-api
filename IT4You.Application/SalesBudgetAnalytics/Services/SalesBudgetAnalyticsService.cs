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
                    Name = "VisÃ£o geral",
                    Description = "Panorama inicial de orÃ§amentos, volume, ticket mÃ©dio e sazonalidade.",
                    PlannedCount = 15,
                    AvailableNowCount = 15,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "overview_total_amount_period", Title = "Valor total de orÃ§amentos por perÃ­odo", Availability = "available_now" },
                        new() { Id = "overview_total_count_period", Title = "Quantidade de orÃ§amentos por perÃ­odo", Availability = "available_now" },
                        new() { Id = "overview_avg_ticket", Title = "Ticket mÃ©dio dos orÃ§amentos", Availability = "available_now" },
                        new() { Id = "overview_amount_by_company", Title = "Valor total por empresa/filial", Availability = "available_now" },
                        new() { Id = "overview_count_by_company", Title = "Quantidade de orÃ§amentos por empresa/filial", Availability = "available_now" },
                        new() { Id = "overview_monthly_evolution", Title = "EvoluÃ§Ã£o mensal de orÃ§amentos", Availability = "available_now" },
                        new() { Id = "overview_weekly_evolution", Title = "EvoluÃ§Ã£o semanal de orÃ§amentos", Availability = "available_now" },
                        new() { Id = "overview_daily_evolution", Title = "EvoluÃ§Ã£o diÃ¡ria de orÃ§amentos", Availability = "available_now" },
                        new() { Id = "overview_current_vs_previous_month", Title = "Comparativo mÃªs atual x mÃªs anterior", Availability = "available_now" },
                        new() { Id = "overview_current_year_vs_previous_year", Title = "Comparativo ano atual x ano anterior", Availability = "available_now" },
                        new() { Id = "overview_top_days_by_volume", Title = "Top dias com maior volume de orÃ§amentos", Availability = "available_now" },
                        new() { Id = "overview_top_months_by_amount", Title = "Top meses com maior valor orÃ§ado", Availability = "available_now" },
                        new() { Id = "overview_month_seasonality", Title = "Sazonalidade de vendas/orÃ§amentos por mÃªs", Availability = "available_now" },
                        new() { Id = "overview_weekday_heatmap", Title = "Mapa de calor por dia da semana", Availability = "available_now" },
                        new() { Id = "overview_month_year_heatmap", Title = "Mapa de calor por mÃªs e ano", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "funnel",
                    Name = "Funil comercial",
                    Description = "Status, conversÃ£o, perdas e gargalos do funil de orÃ§amentos.",
                    PlannedCount = 15,
                    AvailableNowCount = 15,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "funnel_by_status", Title = "Funil por status do orÃ§amento", Availability = "available_now" },
                        new() { Id = "funnel_amount_by_status", Title = "Valor total por status", Availability = "available_now" },
                        new() { Id = "funnel_count_by_status", Title = "Quantidade de orÃ§amentos por status", Availability = "available_now" },
                        new() { Id = "funnel_conversion_percent_by_status", Title = "Percentual de conversÃ£o por status", Availability = "available_now" },
                        new() { Id = "funnel_open_approved_lost", Title = "OrÃ§amentos em aberto x aprovados x perdidos", Availability = "available_now" },
                        new() { Id = "funnel_pending_amount", Title = "Valor parado em orÃ§amentos pendentes", Availability = "available_now" },
                        new() { Id = "funnel_approval_rate", Title = "Taxa de aprovaÃ§Ã£o de orÃ§amentos", Availability = "available_now" },
                        new() { Id = "funnel_loss_cancel_rate", Title = "Taxa de perda/cancelamento", Availability = "available_now" },
                        new() { Id = "funnel_conversion_evolution", Title = "EvoluÃ§Ã£o da conversÃ£o ao longo do tempo", Availability = "available_now" },
                        new() { Id = "funnel_conversion_by_seller", Title = "ConversÃ£o por vendedor", Availability = "available_now" },
                        new() { Id = "funnel_conversion_by_customer", Title = "ConversÃ£o por cliente", Availability = "available_now" },
                        new() { Id = "funnel_conversion_by_origin", Title = "ConversÃ£o por origem", Availability = "available_now" },
                        new() { Id = "funnel_conversion_by_geo", Title = "ConversÃ£o por cidade/UF", Availability = "available_now" },
                        new() { Id = "funnel_conversion_by_payment", Title = "ConversÃ£o por condiÃ§Ã£o de pagamento", Availability = "available_now" },
                        new() { Id = "funnel_blocking_status_ranking", Title = "Ranking de status que mais travam vendas", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "seller",
                    Name = "Vendedores",
                    Description = "Performance comercial, ranking, markup, desconto e evoluÃ§Ã£o por vendedor.",
                    PlannedCount = 20,
                    AvailableNowCount = 20,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "seller_total_amount", Title = "Valor total orÃ§ado por vendedor", Availability = "available_now" },
                        new() { Id = "seller_total_count", Title = "Quantidade de orÃ§amentos por vendedor", Availability = "available_now" },
                        new() { Id = "seller_avg_ticket", Title = "Ticket mÃ©dio por vendedor", Availability = "available_now" },
                        new() { Id = "seller_conversion", Title = "ConversÃ£o por vendedor", Availability = "available_now" },
                        new() { Id = "seller_avg_discount", Title = "Desconto mÃ©dio concedido por vendedor", Availability = "available_now" },
                        new() { Id = "seller_avg_markup", Title = "Markup mÃ©dio por vendedor", Availability = "available_now" },
                        new() { Id = "seller_avg_surcharge", Title = "AcrÃ©scimo mÃ©dio por vendedor", Availability = "available_now" },
                        new() { Id = "seller_avg_freight", Title = "Valor de frete mÃ©dio por vendedor", Availability = "available_now" },
                        new() { Id = "seller_ranking_amount", Title = "Ranking de vendedores por valor total", Availability = "available_now" },
                        new() { Id = "seller_ranking_count", Title = "Ranking de vendedores por quantidade", Availability = "available_now" },
                        new() { Id = "seller_ranking_ticket", Title = "Ranking de vendedores por ticket mÃ©dio", Availability = "available_now" },
                        new() { Id = "seller_ranking_markup", Title = "Ranking de vendedores por margem/markup", Availability = "available_now" },
                        new() { Id = "seller_most_lost", Title = "Vendedores com mais orÃ§amentos perdidos", Availability = "available_now" },
                        new() { Id = "seller_most_approved", Title = "Vendedores com mais orÃ§amentos aprovados", Availability = "available_now" },
                        new() { Id = "seller_monthly_evolution", Title = "EvoluÃ§Ã£o mensal por vendedor", Availability = "available_now" },
                        new() { Id = "seller_comparison", Title = "Comparativo entre vendedores", Availability = "available_now" },
                        new() { Id = "seller_share_total", Title = "ParticipaÃ§Ã£o de cada vendedor no faturamento", Availability = "available_now" },
                        new() { Id = "seller_abc_curve", Title = "Curva ABC de vendedores", Availability = "available_now" },
                        new() { Id = "seller_top_product", Title = "Vendedor x produto mais orÃ§ado", Availability = "available_now" },
                        new() { Id = "seller_top_customer", Title = "Vendedor x cliente mais atendido", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "customer",
                    Name = "Clientes",
                    Description = "Ticket, recorrÃªncia, conversÃ£o, origem e distribuiÃ§Ã£o geogrÃ¡fica dos clientes.",
                    PlannedCount = 20,
                    AvailableNowCount = 20,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "customer_top_amount", Title = "Top clientes por valor orÃ§ado", Availability = "available_now" },
                        new() { Id = "customer_top_count", Title = "Top clientes por quantidade de orÃ§amentos", Availability = "available_now" },
                        new() { Id = "customer_avg_ticket", Title = "Ticket mÃ©dio por cliente", Availability = "available_now" },
                        new() { Id = "customer_recurring", Title = "Clientes recorrentes", Availability = "available_now" },
                        new() { Id = "customer_new_period", Title = "Clientes novos por perÃ­odo", Availability = "available_now" },
                        new() { Id = "customer_inactive_recent", Title = "Clientes sem orÃ§amento recente", Availability = "available_now" },
                        new() { Id = "customer_highest_discount", Title = "Clientes com maior desconto recebido", Availability = "available_now" },
                        new() { Id = "customer_highest_markup", Title = "Clientes com maior markup", Availability = "available_now" },
                        new() { Id = "customer_highest_open_amount", Title = "Clientes com maior valor em aberto", Availability = "available_now" },
                        new() { Id = "customer_highest_conversion", Title = "Clientes com maior taxa de conversÃ£o", Availability = "available_now" },
                        new() { Id = "customer_low_conversion", Title = "Clientes com baixa conversÃ£o", Availability = "available_now" },
                        new() { Id = "customer_abc_curve", Title = "Curva ABC de clientes", Availability = "available_now" },
                        new() { Id = "customer_top_share", Title = "ParticipaÃ§Ã£o dos principais clientes no total", Availability = "available_now" },
                        new() { Id = "customer_evolution", Title = "EvoluÃ§Ã£o de orÃ§amentos por cliente", Availability = "available_now" },
                        new() { Id = "customer_top_products", Title = "Cliente x produtos mais orÃ§ados", Availability = "available_now" },
                        new() { Id = "customer_responsible_seller", Title = "Cliente x vendedor responsÃ¡vel", Availability = "available_now" },
                        new() { Id = "customer_origin", Title = "Cliente x origem", Availability = "available_now" },
                        new() { Id = "customer_payment_condition", Title = "Cliente x condiÃ§Ã£o de pagamento", Availability = "available_now" },
                        new() { Id = "customer_by_city", Title = "Clientes por cidade", Availability = "available_now" },
                        new() { Id = "customer_by_uf", Title = "Clientes por UF", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "product",
                    Name = "Produtos",
                    Description = "Itens orÃ§ados, demanda, valor unitÃ¡rio, mix e associaÃ§Ã£o entre produtos.",
                    PlannedCount = 25,
                    AvailableNowCount = 25,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "product_top_amount", Title = "Top produtos por valor total", Availability = "available_now" },
                        new() { Id = "product_top_quantity", Title = "Top produtos por quantidade orÃ§ada", Availability = "available_now" },
                        new() { Id = "product_highest_avg_ticket", Title = "Produtos com maior ticket mÃ©dio", Availability = "available_now" },
                        new() { Id = "product_highest_discount", Title = "Produtos com maior desconto aplicado", Availability = "available_now" },
                        new() { Id = "product_highest_markup", Title = "Produtos com maior markup", Availability = "available_now" },
                        new() { Id = "product_highest_surcharge", Title = "Produtos com maior acrÃ©scimo", Availability = "available_now" },
                        new() { Id = "product_most_quoted_period", Title = "Produtos mais orÃ§ados por perÃ­odo", Availability = "available_now" },
                        new() { Id = "product_least_quoted", Title = "Produtos menos orÃ§ados", Availability = "available_now" },
                        new() { Id = "product_demand_drop", Title = "Produtos com queda de demanda", Availability = "available_now" },
                        new() { Id = "product_demand_growth", Title = "Produtos com crescimento de demanda", Availability = "available_now" },
                        new() { Id = "product_monthly_evolution", Title = "EvoluÃ§Ã£o mensal por produto", Availability = "available_now" },
                        new() { Id = "product_abc_curve", Title = "Curva ABC de produtos", Availability = "available_now" },
                        new() { Id = "product_share_total", Title = "ParticipaÃ§Ã£o dos produtos no valor total", Availability = "available_now" },
                        new() { Id = "product_by_seller", Title = "Produtos por vendedor", Availability = "available_now" },
                        new() { Id = "product_by_customer", Title = "Produtos por cliente", Availability = "available_now" },
                        new() { Id = "product_by_geo", Title = "Produtos por cidade/UF", Availability = "available_now" },
                        new() { Id = "product_by_company", Title = "Produtos por empresa/filial", Availability = "available_now" },
                        new() { Id = "product_by_origin", Title = "Produtos por origem do orÃ§amento", Availability = "available_now" },
                        new() { Id = "product_highest_gross_unit", Title = "Produtos com maior valor unitÃ¡rio bruto", Availability = "available_now" },
                        new() { Id = "product_highest_net_unit", Title = "Produtos com maior valor unitÃ¡rio lÃ­quido", Availability = "available_now" },
                        new() { Id = "product_gross_net_gap", Title = "DiferenÃ§a entre valor bruto e lÃ­quido", Availability = "available_now" },
                        new() { Id = "product_avg_quantity_per_item", Title = "Quantidade mÃ©dia por item", Availability = "available_now" },
                        new() { Id = "product_avg_value_per_item", Title = "Valor mÃ©dio por item", Availability = "available_now" },
                        new() { Id = "product_mix_per_budget", Title = "Mix de produtos por orÃ§amento", Availability = "available_now" },
                        new() { Id = "product_cooccurrence", Title = "Produtos que mais aparecem juntos", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "margin",
                    Name = "Descontos e margem",
                    Description = "Desconto, acrÃ©scimo, markup e orÃ§amentos com possÃ­vel margem ruim.",
                    PlannedCount = 25,
                    AvailableNowCount = 25,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "margin_total_discount", Title = "Valor total de descontos concedidos", Availability = "available_now" },
                        new() { Id = "margin_avg_discount_percent", Title = "Percentual mÃ©dio de desconto", Availability = "available_now" },
                        new() { Id = "margin_discount_by_seller", Title = "Desconto por vendedor", Availability = "available_now" },
                        new() { Id = "margin_discount_by_customer", Title = "Desconto por cliente", Availability = "available_now" },
                        new() { Id = "margin_discount_by_product", Title = "Desconto por produto", Availability = "available_now" },
                        new() { Id = "margin_discount_by_origin", Title = "Desconto por origem", Availability = "available_now" },
                        new() { Id = "margin_discount_by_payment", Title = "Desconto por condiÃ§Ã£o de pagamento", Availability = "available_now" },
                        new() { Id = "margin_highest_discount_ranking", Title = "Ranking de maiores descontos", Availability = "available_now" },
                        new() { Id = "margin_above_avg_discount_budgets", Title = "OrÃ§amentos com desconto acima da mÃ©dia", Availability = "available_now" },
                        new() { Id = "margin_discount_impact_total", Title = "Impacto do desconto no valor total", Availability = "available_now" },
                        new() { Id = "margin_discount_vs_conversion", Title = "RelaÃ§Ã£o desconto x conversÃ£o", Availability = "available_now" },
                        new() { Id = "margin_discount_vs_seller", Title = "RelaÃ§Ã£o desconto x vendedor", Availability = "available_now" },
                        new() { Id = "margin_total_surcharge", Title = "Valor total de acrÃ©scimos", Availability = "available_now" },
                        new() { Id = "margin_avg_surcharge_percent", Title = "Percentual mÃ©dio de acrÃ©scimo", Availability = "available_now" },
                        new() { Id = "margin_surcharge_by_seller", Title = "AcrÃ©scimo por vendedor", Availability = "available_now" },
                        new() { Id = "margin_surcharge_by_customer", Title = "AcrÃ©scimo por cliente", Availability = "available_now" },
                        new() { Id = "margin_surcharge_by_product", Title = "AcrÃ©scimo por produto", Availability = "available_now" },
                        new() { Id = "margin_avg_markup_general", Title = "Markup mÃ©dio geral", Availability = "available_now" },
                        new() { Id = "margin_markup_by_seller", Title = "Markup por vendedor", Availability = "available_now" },
                        new() { Id = "margin_markup_by_product", Title = "Markup por produto", Availability = "available_now" },
                        new() { Id = "margin_markup_by_customer", Title = "Markup por cliente", Availability = "available_now" },
                        new() { Id = "margin_markup_by_origin", Title = "Markup por origem", Availability = "available_now" },
                        new() { Id = "margin_low_markup_budgets", Title = "OrÃ§amentos com markup baixo", Availability = "available_now" },
                        new() { Id = "margin_possible_bad_margin_budgets", Title = "OrÃ§amentos com possÃ­vel margem ruim", Availability = "available_now" },
                        new() { Id = "margin_gross_vs_net", Title = "Comparativo valor bruto x valor lÃ­quido", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "source",
                    Name = "Origem",
                    Description = "Canais de venda, participaÃ§Ã£o, ticket e conversÃ£o por origem.",
                    PlannedCount = 15,
                    AvailableNowCount = 15,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "source_total_amount", Title = "Valor total por origem", Availability = "available_now" },
                        new() { Id = "source_total_count", Title = "Quantidade de orÃ§amentos por origem", Availability = "available_now" },
                        new() { Id = "source_avg_ticket", Title = "Ticket mÃ©dio por origem", Availability = "available_now" },
                        new() { Id = "source_conversion", Title = "ConversÃ£o por origem", Availability = "available_now" },
                        new() { Id = "source_highest_avg_discount", Title = "Origem com maior desconto mÃ©dio", Availability = "available_now" },
                        new() { Id = "source_highest_markup", Title = "Origem com maior markup", Availability = "available_now" },
                        new() { Id = "source_evolution", Title = "EvoluÃ§Ã£o de origens por perÃ­odo", Availability = "available_now" },
                        new() { Id = "source_share_total", Title = "ParticipaÃ§Ã£o de cada origem no total", Availability = "available_now" },
                        new() { Id = "source_by_seller", Title = "Origem x vendedor", Availability = "available_now" },
                        new() { Id = "source_by_product", Title = "Origem x produto", Availability = "available_now" },
                        new() { Id = "source_by_customer", Title = "Origem x cliente", Availability = "available_now" },
                        new() { Id = "source_by_geo", Title = "Origem x cidade/UF", Availability = "available_now" },
                        new() { Id = "source_best_channels", Title = "Ranking de melhores canais de venda", Availability = "available_now" },
                        new() { Id = "source_high_volume_low_conversion", Title = "Canais com muito orÃ§amento e pouca conversÃ£o", Availability = "available_now" },
                        new() { Id = "source_low_volume_high_ticket", Title = "Canais com menos volume, mas maior ticket", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "geo",
                    Name = "Geografia",
                    Description = "DistribuiÃ§Ã£o por UF, cidade, regiÃ£o, vendedor e oportunidade geogrÃ¡fica.",
                    PlannedCount = 17,
                    AvailableNowCount = 17,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "geo_amount_by_uf", Title = "Valor total por UF", Availability = "available_now" },
                        new() { Id = "geo_count_by_uf", Title = "Quantidade de orÃ§amentos por UF", Availability = "available_now" },
                        new() { Id = "geo_avg_ticket_by_uf", Title = "Ticket mÃ©dio por UF", Availability = "available_now" },
                        new() { Id = "geo_conversion_by_uf", Title = "ConversÃ£o por UF", Availability = "available_now" },
                        new() { Id = "geo_amount_by_city", Title = "Valor total por cidade", Availability = "available_now" },
                        new() { Id = "geo_count_by_city", Title = "Quantidade por cidade", Availability = "available_now" },
                        new() { Id = "geo_top_cities_count", Title = "Ranking de cidades com mais orÃ§amentos", Availability = "available_now" },
                        new() { Id = "geo_top_cities_ticket", Title = "Ranking de cidades com maior ticket mÃ©dio", Availability = "available_now" },
                        new() { Id = "geo_state_heatmap", Title = "Mapa de calor por estado", Availability = "available_now" },
                        new() { Id = "geo_city_heatmap", Title = "Mapa de calor por cidade", Availability = "available_now" },
                        new() { Id = "geo_seller_by_region", Title = "Vendedor por regiÃ£o", Availability = "available_now" },
                        new() { Id = "geo_top_product_by_uf", Title = "Produto mais orÃ§ado por UF", Availability = "available_now" },
                        new() { Id = "geo_customer_by_region", Title = "Cliente por regiÃ£o", Availability = "available_now" },
                        new() { Id = "geo_origin_by_region", Title = "Origem por regiÃ£o", Availability = "available_now" },
                        new() { Id = "geo_highest_avg_discount_regions", Title = "RegiÃµes com maior desconto mÃ©dio", Availability = "available_now" },
                        new() { Id = "geo_highest_markup_regions", Title = "RegiÃµes com maior markup", Availability = "available_now" },
                        new() { Id = "geo_growth_opportunity_regions", Title = "RegiÃµes com maior oportunidade de crescimento", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "payment",
                    Name = "CondiÃ§Ã£o de pagamento",
                    Description = "Uso, ticket, desconto, markup e aprovaÃ§Ã£o por condiÃ§Ã£o de pagamento.",
                    PlannedCount = 12,
                    AvailableNowCount = 12,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "payment_total_amount", Title = "Valor total por condiÃ§Ã£o de pagamento", Availability = "available_now" },
                        new() { Id = "payment_total_count", Title = "Quantidade por condiÃ§Ã£o de pagamento", Availability = "available_now" },
                        new() { Id = "payment_avg_ticket", Title = "Ticket mÃ©dio por condiÃ§Ã£o de pagamento", Availability = "available_now" },
                        new() { Id = "payment_conversion", Title = "ConversÃ£o por condiÃ§Ã£o de pagamento", Availability = "available_now" },
                        new() { Id = "payment_avg_discount", Title = "Desconto mÃ©dio por condiÃ§Ã£o de pagamento", Availability = "available_now" },
                        new() { Id = "payment_avg_markup", Title = "Markup mÃ©dio por condiÃ§Ã£o de pagamento", Availability = "available_now" },
                        new() { Id = "payment_most_used", Title = "CondiÃ§Ãµes de pagamento mais usadas", Availability = "available_now" },
                        new() { Id = "payment_by_seller", Title = "CondiÃ§Ã£o de pagamento x vendedor", Availability = "available_now" },
                        new() { Id = "payment_by_customer", Title = "CondiÃ§Ã£o de pagamento x cliente", Availability = "available_now" },
                        new() { Id = "payment_by_product", Title = "CondiÃ§Ã£o de pagamento x produto", Availability = "available_now" },
                        new() { Id = "payment_by_origin", Title = "CondiÃ§Ã£o de pagamento x origem", Availability = "available_now" },
                        new() { Id = "payment_vs_approval", Title = "CondiÃ§Ã£o de pagamento x aprovaÃ§Ã£o", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "freight",
                    Name = "Frete",
                    Description = "Valor de frete, tipo, impacto no ticket e relaÃ§Ã£o com conversÃ£o.",
                    PlannedCount = 11,
                    AvailableNowCount = 12,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "freight_total_amount", Title = "Valor total de frete", Availability = "available_now" },
                        new() { Id = "freight_avg_per_budget", Title = "Frete mÃ©dio por orÃ§amento", Availability = "available_now" },
                        new() { Id = "freight_by_seller", Title = "Frete por vendedor", Availability = "available_now" },
                        new() { Id = "freight_by_customer", Title = "Frete por cliente", Availability = "available_now" },
                        new() { Id = "freight_by_geo", Title = "Frete por cidade/UF", Availability = "available_now" },
                        new() { Id = "freight_by_type", Title = "Frete por tipo de frete", Availability = "available_now" },
                        new() { Id = "freight_ratio_total", Title = "Frete em relaÃ§Ã£o ao valor total", Availability = "available_now" },
                        new() { Id = "freight_high_budgets", Title = "OrÃ§amentos com frete alto", Availability = "available_now" },
                        new() { Id = "freight_vs_conversion", Title = "RelaÃ§Ã£o frete x conversÃ£o", Availability = "available_now" },
                        new() { Id = "freight_most_used_type", Title = "Tipo de frete mais usado", Availability = "available_now" },
                        new() { Id = "freight_avg_ticket_by_type", Title = "Ticket mÃ©dio por tipo de frete", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "executive",
                    Name = "Diretoria",
                    Description = "Indicadores consolidados, pipeline, alertas e leitura executiva de vendas.",
                    PlannedCount = 20,
                    AvailableNowCount = 20,
                    NeedsNewViewCount = 2,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "exec_dashboard", Title = "Dashboard executivo de vendas", Availability = "available_now" },
                        new() { Id = "exec_total_revenue_budget", Title = "Receita/orÃ§amento total por perÃ­odo", Availability = "available_now" },
                        new() { Id = "exec_goal_vs_actual", Title = "Meta x realizado", Availability = "needs_new_view" },
                        new() { Id = "exec_sales_forecast", Title = "Forecast de vendas", Availability = "needs_new_view" },
                        new() { Id = "exec_open_pipeline", Title = "Pipeline comercial em aberto", Availability = "available_now" },
                        new() { Id = "exec_opportunity_ranking", Title = "Ranking de oportunidades", Availability = "available_now" },
                        new() { Id = "exec_monthly_growth", Title = "EvoluÃ§Ã£o de crescimento mensal", Availability = "available_now" },
                        new() { Id = "exec_seller_share", Title = "ParticipaÃ§Ã£o por vendedor", Availability = "available_now" },
                        new() { Id = "exec_customer_share", Title = "ParticipaÃ§Ã£o por cliente", Availability = "available_now" },
                        new() { Id = "exec_product_share", Title = "ParticipaÃ§Ã£o por produto", Availability = "available_now" },
                        new() { Id = "exec_region_share", Title = "ParticipaÃ§Ã£o por regiÃ£o", Availability = "available_now" },
                        new() { Id = "exec_origin_share", Title = "ParticipaÃ§Ã£o por origem", Availability = "available_now" },
                        new() { Id = "exec_avg_markup", Title = "Margem/markup mÃ©dio consolidado", Availability = "available_now" },
                        new() { Id = "exec_total_discount", Title = "Desconto total concedido", Availability = "available_now" },
                        new() { Id = "exec_lost_opportunities", Title = "Oportunidades perdidas", Availability = "available_now" },
                        new() { Id = "exec_negotiation_opportunities", Title = "Oportunidades em negociaÃ§Ã£o", Availability = "available_now" },
                        new() { Id = "exec_strategic_customers", Title = "Clientes estratÃ©gicos", Availability = "available_now" },
                        new() { Id = "exec_strategic_products", Title = "Produtos estratÃ©gicos", Availability = "available_now" },
                        new() { Id = "exec_strategic_channels", Title = "Canais estratÃ©gicos", Availability = "available_now" },
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
                        new() { Id = "insight_customers_high_conversion_chance", Title = "Clientes com maior chance de conversÃ£o", Availability = "available_now" },
                        new() { Id = "insight_frequent_customers", Title = "Clientes que orÃ§am com frequÃªncia", Availability = "available_now" },
                        new() { Id = "insight_customers_stopped_quoting", Title = "Clientes que pararam de orÃ§ar", Availability = "available_now" },
                        new() { Id = "insight_recommended_products_by_customer", Title = "Produtos mais indicados por cliente", Availability = "available_now" },
                        new() { Id = "insight_top_products_by_region", Title = "Produtos mais vendidos por regiÃ£o", Availability = "available_now" },
                        new() { Id = "insight_high_acceptance_products", Title = "Produtos com maior aceitaÃ§Ã£o", Availability = "available_now" },
                        new() { Id = "insight_pending_followup_budgets", Title = "OrÃ§amentos pendentes de follow-up", Availability = "available_now" },
                        new() { Id = "insight_old_open_budgets", Title = "OrÃ§amentos antigos ainda em aberto", Availability = "available_now" },
                        new() { Id = "insight_high_value_no_return", Title = "OrÃ§amentos de alto valor sem retorno", Availability = "available_now" },
                        new() { Id = "insight_high_ticket_customers", Title = "Clientes com alto ticket mÃ©dio", Availability = "available_now" },
                        new() { Id = "insight_discount_sensitive_customers", Title = "Clientes sensÃ­veis a desconto", Availability = "available_now" },
                        new() { Id = "insight_low_discount_customers", Title = "Clientes que compram sem muito desconto", Availability = "available_now" },
                        new() { Id = "insight_best_origin_by_seller", Title = "Melhor origem para cada vendedor", Availability = "available_now" },
                        new() { Id = "insight_best_product_by_seller", Title = "Melhor produto para cada vendedor", Availability = "available_now" },
                        new() { Id = "insight_best_region_by_seller", Title = "Melhor regiÃ£o para cada vendedor", Availability = "available_now" },
                        new() { Id = "insight_seller_vs_team_avg", Title = "Comparativo do vendedor com a equipe", Availability = "available_now" },
                        new() { Id = "insight_personal_seller_ranking", Title = "Ranking pessoal do vendedor", Availability = "available_now" },
                        new() { Id = "insight_individual_monthly_evolution", Title = "EvoluÃ§Ã£o mensal individual", Availability = "available_now" },
                        new() { Id = "insight_underused_products_by_seller", Title = "Produtos que o vendedor vende pouco", Availability = "available_now" },
                        new() { Id = "insight_repurchase_potential_customers", Title = "Clientes com potencial de recompra", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "future_data",
                    Name = "Novas informaÃ§Ãµes",
                    Description = "GrÃ¡ficos dependentes de pedido, faturamento, estoque, custo, metas e marketing.",
                    PlannedCount = 30,
                    AvailableNowCount = 3,
                    NeedsNewViewCount = 27,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "future_budget_vs_sold", Title = "OrÃ§ado x vendido/faturado", Availability = "available_now" },
                        new() { Id = "future_budget_converted_to_order", Title = "OrÃ§amento convertido em pedido", Availability = "available_now" },
                        new() { Id = "future_avg_conversion_time", Title = "Tempo mÃ©dio de conversÃ£o", Availability = "available_now" },
                        new() { Id = "future_issue_to_approval_time", Title = "Tempo mÃ©dio entre emissÃ£o e aprovaÃ§Ã£o", Availability = "needs_new_view" },
                        new() { Id = "future_loss_reason", Title = "Motivo de perda do orÃ§amento", Availability = "needs_new_view" },
                        new() { Id = "future_cancel_reason", Title = "Motivo de cancelamento", Availability = "needs_new_view" },
                        new() { Id = "future_goal_by_seller", Title = "Meta por vendedor", Availability = "needs_new_view" },
                        new() { Id = "future_goal_by_company", Title = "Meta por filial", Availability = "needs_new_view" },
                        new() { Id = "future_goal_by_product", Title = "Meta por produto", Availability = "needs_new_view" },
                        new() { Id = "future_goal_by_region", Title = "Meta por regiÃ£o", Availability = "needs_new_view" },
                        new() { Id = "future_real_margin_by_product", Title = "Margem real por produto", Availability = "needs_new_view" },
                        new() { Id = "future_gross_profit", Title = "Lucro bruto", Availability = "needs_new_view" },
                        new() { Id = "future_cogs", Title = "Custo do produto vendido", Availability = "needs_new_view" },
                        new() { Id = "future_available_stock", Title = "Estoque disponÃ­vel", Availability = "needs_new_view" },
                        new() { Id = "future_stockout", Title = "Ruptura de estoque", Availability = "needs_new_view" },
                        new() { Id = "future_no_stock_lost_sales", Title = "Produtos sem estoque que perderam venda", Availability = "needs_new_view" },
                        new() { Id = "future_inventory_turnover", Title = "Giro de estoque", Availability = "needs_new_view" },
                        new() { Id = "future_demand_forecast", Title = "PrevisÃ£o de demanda", Availability = "needs_new_view" },
                        new() { Id = "future_seller_commission", Title = "ComissÃ£o por vendedor", Availability = "needs_new_view" },
                        new() { Id = "future_profitability_by_seller", Title = "Rentabilidade por vendedor", Availability = "needs_new_view" },
                        new() { Id = "future_profitability_by_customer", Title = "Rentabilidade por cliente", Availability = "needs_new_view" },
                        new() { Id = "future_profitability_by_product", Title = "Rentabilidade por produto", Availability = "needs_new_view" },
                        new() { Id = "future_customer_default", Title = "InadimplÃªncia por cliente", Availability = "needs_new_view" },
                        new() { Id = "future_avg_payment_term", Title = "Prazo mÃ©dio de pagamento", Availability = "needs_new_view" },
                        new() { Id = "future_customer_ltv", Title = "Lifetime Value do cliente", Availability = "needs_new_view" },
                        new() { Id = "future_customer_churn", Title = "Churn de clientes", Availability = "needs_new_view" },
                        new() { Id = "future_customer_repurchase", Title = "Recompra por cliente", Availability = "needs_new_view" },
                        new() { Id = "future_avg_purchase_frequency", Title = "FrequÃªncia mÃ©dia de compra", Availability = "needs_new_view" },
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
                    AvailableNowCount = 25,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "kpi_total_budget_amount", Title = "Valor total orÃ§ado", Availability = "available_now" },
                        new() { Id = "kpi_budget_count", Title = "Quantidade de orÃ§amentos", Availability = "available_now" },
                        new() { Id = "kpi_avg_ticket", Title = "Ticket mÃ©dio", Availability = "available_now" },
                        new() { Id = "kpi_conversion_rate", Title = "Taxa de conversÃ£o", Availability = "available_now" },
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
                        new() { Id = "kpi_avg_discount", Title = "Desconto mÃ©dio", Availability = "available_now" },
                        new() { Id = "kpi_avg_markup", Title = "Markup mÃ©dio", Availability = "available_now" },
                        new() { Id = "kpi_avg_freight", Title = "Frete mÃ©dio", Availability = "available_now" },
                        new() { Id = "kpi_most_quoted_product", Title = "Produto mais orÃ§ado", Availability = "available_now" },
                        new() { Id = "kpi_highest_potential_customer", Title = "Cliente com maior potencial", Availability = "available_now" },
                        new() { Id = "kpi_seller_highest_growth", Title = "Vendedor com maior crescimento", Availability = "available_now" },
                        new() { Id = "kpi_seller_highest_drop", Title = "Vendedor com maior queda", Availability = "available_now" },
                        new() { Id = "kpi_product_highest_growth", Title = "Produto com maior crescimento", Availability = "available_now" },
                        new() { Id = "kpi_product_highest_drop", Title = "Produto com maior queda", Availability = "available_now" },
                        new() { Id = "kpi_channel_highest_conversion", Title = "Canal com maior conversÃ£o", Availability = "available_now" },
                        new() { Id = "kpi_channel_lowest_conversion", Title = "Canal com menor conversÃ£o", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "velocity",
                    Name = "Velocidade de vendas",
                    Description = "Ciclo mÃ©dio de vendas, gargalos de status e aceleraÃ§Ã£o de conversÃ£o.",
                    PlannedCount = 5,
                    AvailableNowCount = 4,
                    NeedsNewViewCount = 1,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "velocity_avg_cycle_time", Title = "Tempo mÃ©dio do ciclo de vendas", Availability = "available_now" },
                        new() { Id = "velocity_by_seller", Title = "Velocidade de conversÃ£o por vendedor", Availability = "available_now" },
                        new() { Id = "velocity_by_product", Title = "Velocidade por produto", Availability = "available_now" },
                        new() { Id = "velocity_conversion_acceleration", Title = "AceleraÃ§Ã£o de conversÃ£o", Availability = "available_now" },
                        new() { Id = "velocity_status_bottleneck_time", Title = "Tempo mÃ©dio de permanÃªncia em status", Availability = "needs_new_view" },
                    }
                },
                new()
                {
                    Id = "risk",
                    Name = "ConcentraÃ§Ã£o de Risco",
                    Description = "DependÃªncia de top clientes, produtos, vendedores e dependÃªncia de desconto.",
                    PlannedCount = 5,
                    AvailableNowCount = 5,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "risk_customer_concentration", Title = "ConcentraÃ§Ã£o nos Top Clientes", Availability = "available_now" },
                        new() { Id = "risk_product_dependence", Title = "DependÃªncia de produtos", Availability = "available_now" },
                        new() { Id = "risk_seller_concentration", Title = "ConcentraÃ§Ã£o em vendedores", Availability = "available_now" },
                        new() { Id = "risk_geo_concentration", Title = "ConcentraÃ§Ã£o de risco por regiÃ£o", Availability = "available_now" },
                        new() { Id = "risk_high_discount_volume", Title = "Vendas dependentes de altos descontos", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "efficiency",
                    Name = "EficiÃªncia Operacional",
                    Description = "Taxa de ganho vs tempo, abandono e volume orÃ§ado vs fechado.",
                    PlannedCount = 4,
                    AvailableNowCount = 4,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "efficiency_win_rate_vs_time", Title = "Taxa de ganho vs Idade do orÃ§amento", Availability = "available_now" },
                        new() { Id = "efficiency_quote_to_close_ratio", Title = "Volume orÃ§ado vs Volume fechado", Availability = "available_now" },
                        new() { Id = "efficiency_abandonment_rate", Title = "Taxa de abandono", Availability = "available_now" },
                        new() { Id = "efficiency_avg_items_per_ticket", Title = "EficiÃªncia de Mix", Availability = "available_now" },
                    }
                },
                new()
                {
                    Id = "predictive",
                    Name = "AnÃ¡lise Preditiva Simples",
                    Description = "PrevisÃ£o de fechamento baseada em funil, sazonalidade e risco de inatividade.",
                    PlannedCount = 4,
                    AvailableNowCount = 4,
                    NeedsNewViewCount = 0,
                    Highlights = new List<SalesBudgetChartPreviewDto>
                    {
                        new() { Id = "predictive_sales_forecast", Title = "PrevisÃ£o de fechamento (Funil Atual)", Availability = "available_now" },
                        new() { Id = "predictive_churn_risk", Title = "Risco de Churn (Inatividade)", Availability = "available_now" },
                        new() { Id = "predictive_seasonal_trend", Title = "TendÃªncia Sazonal", Availability = "available_now" },
                        new() { Id = "predictive_high_probability_deals", Title = "OrÃ§amentos com alta probabilidade", Availability = "available_now" },
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

    public Task<List<SalesBudgetChartQueryDetailsItemDto>> GetChartQueryDetailsAsync(
        SalesBudgetChartQueryDetailsRequestDto request
    )
    {
        return _repository.GetChartQueryDetailsAsync(request);
    }
}

