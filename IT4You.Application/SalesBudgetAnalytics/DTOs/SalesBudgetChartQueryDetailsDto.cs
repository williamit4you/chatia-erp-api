using System;
using System.Collections.Generic;

namespace IT4You.Application.SalesBudgetAnalytics.DTOs;

public class SalesBudgetChartQueryDetailsRequestDto
{
    public List<string> ChartIds { get; set; } = new();
    public DateTime? StartDate { get; set; }
    public DateTime? EndDate { get; set; }
}

public class SalesBudgetChartQueryDetailsItemDto
{
    public string ChartId { get; set; } = string.Empty;
    public List<string> SqlQueries { get; set; } = new();
    public List<string> Rules { get; set; } = new();
}

public class SalesBudgetChartQueryDetailsResponseDto
{
    public List<SalesBudgetChartQueryDetailsItemDto> Items { get; set; } = new();
}

