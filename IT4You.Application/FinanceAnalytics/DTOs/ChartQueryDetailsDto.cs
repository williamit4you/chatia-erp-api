using System;
using System.Collections.Generic;

namespace IT4You.Application.FinanceAnalytics.DTOs
{
    public class ChartQueryDetailsRequestDto
    {
        public List<string> ChartIds { get; set; } = new();
        public DateTime? StartDate { get; set; }
        public DateTime? EndDate { get; set; }
    }

    public class ChartQueryDetailsItemDto
    {
        public string ChartId { get; set; }
        public List<string> SqlQueries { get; set; } = new();
        public List<string> Rules { get; set; } = new();
    }

    public class ChartQueryDetailsResponseDto
    {
        public List<ChartQueryDetailsItemDto> Items { get; set; } = new();
    }
}

