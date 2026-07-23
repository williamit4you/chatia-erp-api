using System.Collections.Generic;

namespace IT4You.Application.FinanceAnalytics.DTOs
{
    public class FinanceCompanyOptionDto
    {
        public string Id { get; set; } = string.Empty;
        public string Label { get; set; } = string.Empty;
        public string? CpfCnpj { get; set; }
        public string? City { get; set; }
        public string? Uf { get; set; }
    }

    public class FinanceCompaniesResponseDto
    {
        public List<FinanceCompanyOptionDto> Items { get; set; } = new();
    }
}
