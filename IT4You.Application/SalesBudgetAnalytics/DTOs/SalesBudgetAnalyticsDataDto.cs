using System;
using System.Collections.Generic;

namespace IT4You.Application.SalesBudgetAnalytics.DTOs;

public class SalesBudgetFilterDto
{
    public DateTime? StartDate { get; set; }
    public DateTime? EndDate { get; set; }
}

public class SalesBudgetKpiRequestDto
{
    public SalesBudgetFilterDto Filters { get; set; } = new();
    public List<string>? KpiIds { get; set; }
}

public class SalesBudgetKpiResponseDto
{
    public List<SalesBudgetKpiItemDto> Items { get; set; } = new();
}

public class SalesBudgetKpiItemDto
{
    public string KpiId { get; set; } = string.Empty;
    public string Label { get; set; } = string.Empty;
    public decimal? Value { get; set; }
    public string? TextValue { get; set; }
    public string Format { get; set; } = "number";
    public string? Warning { get; set; }
}

public class SalesBudgetChartBatchRequestDto
{
    public List<string> ChartIds { get; set; } = new();
    public SalesBudgetFilterDto Filters { get; set; } = new();
}

public class SalesBudgetChartBatchResponseDto
{
    public List<SalesBudgetChartDatasetDto> Items { get; set; } = new();
}

public class SalesBudgetChartDatasetDto
{
    public string ChartId { get; set; } = string.Empty;
    public string Title { get; set; } = string.Empty;
    public string Visualization { get; set; } = string.Empty;
    public List<SalesBudgetChartPointDto> Data { get; set; } = new();
    public Dictionary<string, decimal> Totals { get; set; } = new();
    public SalesBudgetChartMetaDto Meta { get; set; } = new();
}

public class SalesBudgetChartPointDto
{
    public string Label { get; set; } = string.Empty;
    public decimal? Value { get; set; }
    public decimal? Amount { get; set; }
    public decimal? Count { get; set; }
    public decimal? Percentage { get; set; }
    public string? Date { get; set; }
}

public class SalesBudgetChartMetaDto
{
    public string Source { get; set; } = "header";
    public string DateField { get; set; } = "EMISSAO";
    public DateTime GeneratedAt { get; set; } = DateTime.UtcNow;
    public List<string> Warnings { get; set; } = new();
}
