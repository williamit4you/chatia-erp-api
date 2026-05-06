using System;
using System.Collections.Generic;

namespace IT4You.Application.FinanceAnalytics.DTOs
{
    public class ChartExportRequestDto
    {
        public string ChartId { get; set; } = string.Empty;
        public DateTime? StartDate { get; set; }
        public DateTime? EndDate { get; set; }
        public string? EntityValue { get; set; }
        public string Format { get; set; } = "csv"; // csv | xlsx (future)
    }

    public class ChartSelectionDto
    {
        // kind: category | time_bucket | range_bucket | geo_uf
        public string Kind { get; set; } = string.Empty;

        // category
        public string? Key { get; set; }
        public string? Label { get; set; }

        // time_bucket
        public string? Bucket { get; set; } // day | month
        public string? Value { get; set; } // 2026-04 or 2026-04-01

        // geo_uf
        public string? Uf { get; set; }
    }

    public class ChartDrilldownRequestDto
    {
        public string ChartId { get; set; } = string.Empty;
        public DateTime? StartDate { get; set; }
        public DateTime? EndDate { get; set; }
        public string? EntityValue { get; set; }
        public ChartSelectionDto Selection { get; set; } = new();
        public int Page { get; set; } = 1;
        public int PageSize { get; set; } = 50;
        public string? SortField { get; set; }
        public string? SortDirection { get; set; } // asc | desc
    }

    public class DrilldownColumnDto
    {
        public string Key { get; set; } = string.Empty;
        public string Label { get; set; } = string.Empty;
        public string? Kind { get; set; } // text | currency | date | number
    }

    public class DrilldownMetaDto
    {
        public int Page { get; set; }
        public int PageSize { get; set; }
        public int? Total { get; set; }
    }

    public class ChartDrilldownResponseDto
    {
        public List<DrilldownColumnDto> Columns { get; set; } = new();
        public List<Dictionary<string, object?>> Rows { get; set; } = new();
        public DrilldownMetaDto Meta { get; set; } = new();
    }
}

