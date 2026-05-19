using System.Collections.Generic;

namespace IT4You.Application.SalesBudgetAnalytics.DTOs;

public class SalesBudgetCatalogResponseDto
{
    public List<SalesBudgetCategoryDto> Categories { get; set; } = new();
}

public class SalesBudgetCategoryDto
{
    public string Id { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public int PlannedCount { get; set; }
    public int AvailableNowCount { get; set; }
    public int NeedsNewViewCount { get; set; }
    public List<SalesBudgetChartPreviewDto> Highlights { get; set; } = new();
}

public class SalesBudgetChartPreviewDto
{
    public string Id { get; set; } = string.Empty;
    public string Title { get; set; } = string.Empty;
    public string Availability { get; set; } = string.Empty;
}
