using System;
using System.Collections.Generic;

namespace IT4You.Application.FinanceAnalytics.DTOs
{
    public class ChartMetricsRequestDto
    {
        public List<string> ChartIds { get; set; } = new();
        public DateTime? StartDate { get; set; }
        public DateTime? EndDate { get; set; }
        public List<string> CompanyIds { get; set; } = new();
    }

    public class ChartMetricsItemDto
    {
        public string ChartId { get; set; } = string.Empty;
        public decimal CurrentValue { get; set; }
        public decimal PreviousValue { get; set; }
        public decimal DeltaAbs { get; set; }
        public decimal? DeltaPct { get; set; }
        public string Direction { get; set; } = "flat"; // up | down | flat
    }

    public class ChartMetricsResponseDto
    {
        public List<ChartMetricsItemDto> Items { get; set; } = new();
    }
}

