using System.ComponentModel;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Data.SqlClient;
using System.Text.Json;
using System.Text;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using IT4You.Application.FinanceAnalytics.Interfaces;
using ClosedXML.Excel;
using IT4You.Application.Interfaces;

namespace IT4You.Application.Plugins;

public class BudgetPlugin : IChatQueryPlugin
{
    private readonly IErpConnectionFactory _connectionFactory;
    private readonly IMemoryCache _cache;

    private const int EXPORT_THRESHOLD = 10;

    public List<string> ExecutedQueries { get; } = new();
    public void ClearExecutedQueries() => ExecutedQueries.Clear();

    public string? LastExportId { get; private set; }
    public int LastExportTotalLinhas { get; private set; }
    public decimal LastExportValorTotal { get; private set; }
    public int AggregateTotalLinhas { get; private set; }
    public decimal AggregateValorTotal { get; private set; }

    public void ClearExportMetadata()
    {
        LastExportId = null;
        LastExportTotalLinhas = 0;
        LastExportValorTotal = 0;
        AggregateTotalLinhas = 0;
        AggregateValorTotal = 0;
    }

    public string? GetExecutedQueriesJson()
    {
        if (ExecutedQueries.Count == 0) return null;
        var options = new JsonSerializerOptions
        {
            Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            WriteIndented = true
        };
        return JsonSerializer.Serialize(ExecutedQueries, options);
    }

    public BudgetPlugin(IErpConnectionFactory connectionFactory, IMemoryCache cache)
    {
        _connectionFactory = connectionFactory;
        _cache = cache;
    }

    private const string ORCAMENTO_VIEW = "VW_SWIA_ORCAMENTO";
    private const string ORCAMENTO_ITEM_VIEW = "VW_SWIA_ORCAMENTO_ITEM";

    [Description("[MODULO: ORCAMENTO] Consulta de orcamentos (cabecalho). Use filtros vazios (\"\") quando nao houver filtro.")]
    public async Task<string> ConsultarOrcamentos(
        [Description("Data inicial de emissao (ISO 8601). Vazio para ignorar.")] string dataInicioISO = "",
        [Description("Data final de emissao (ISO 8601). Vazio para ignorar.")] string dataFimISO = "",
        [Description("Numero do orcamento (OrcNum). Aceita multiplos separados por '|' ou ','. Vazio para ignorar.")] string numeroOrcamento = "",
        [Description("Numero do cliente no orcamento (NUMEROCLIENTE). Vazio para ignorar.")] string numeroCliente = "",
        [Description("Nome do cliente ou nome fantasia. Aceita multiplos separados por '|'. Vazio para ignorar.")] string cliente = "",
        [Description("CPF/CNPJ (somente numeros). Vazio para ignorar.")] string cnpj = "",
        [Description("Sigla do estado (UF). Vazio para ignorar.")] string uf = "",
        [Description("Cidade. Vazio para ignorar.")] string cidade = "",
        [Description("Empresa/filial (nome fantasia). Vazio para ignorar.")] string filial = "",
        [Description("Codigo da empresa (EmpCod). Aceita multiplos separados por '|' ou ','. Vazio para ignorar.")] string codigoEmpresa = "",
        [Description("Status do orcamento. Vazio para ignorar.")] string status = "",
        [Description("Origem do orcamento. Vazio para ignorar.")] string origem = "",
        [Description("Condicao de pagamento. Vazio para ignorar.")] string condicaoPagamento = "",
        [Description("Vendedor. Vazio para ignorar.")] string vendedor = "",
        [Description("Tipo de frete. Vazio para ignorar.")] string tipoFrete = "",
        [Description("Valor total minimo (VALORTOTAL). Vazio para ignorar.")] string valorTotalMinimo = "",
        [Description("Valor total maximo (VALORTOTAL). Vazio para ignorar.")] string valorTotalMaximo = "",
        [Description("Percentual de desconto minimo. Vazio para ignorar.")] string percentualDescontoMinimo = "",
        [Description("Percentual de desconto maximo. Vazio para ignorar.")] string percentualDescontoMaximo = "",
        [Description("Valor de desconto minimo. Vazio para ignorar.")] string valorDescontoMinimo = "",
        [Description("Valor de desconto maximo. Vazio para ignorar.")] string valorDescontoMaximo = "",
        [Description("Markup minimo. Vazio para ignorar.")] string markupMinimo = "",
        [Description("Markup maximo. Vazio para ignorar.")] string markupMaximo = "",
        [Description("Agrupar por: NENHUM, TOTAL, CLIENTE, NOMEFANTASIA, FILIAL, CODEMPRESA, STATUS, ORIGEM, CONDPAG, VENDEDOR, TIPOFRETE, CIDADE, UF, ANO, MES.")] string agrupamento = "NENHUM",
        [Description("Metrica para ordenar/agregar: VALORTOTAL, VALORFRETE, VALORDESCONTO, VALORACRESCIMO, MARKUP, PERCENTUALDESCONTO, PERCENTUALACRESCIMO, QUANTIDADE.")] string metrica = "VALORTOTAL",
        [Description("Limite maximo de registros para listagem (quando agrupamento=NENHUM). Use para top N.")] int limite = 0,
        [Description("Se verdadeiro, ordena por maior metrica/valor.")] bool ordenarPorMaiorValor = false
    )
    {
        var filters = new BudgetOrcamentoFilters(
            dataInicioISO, dataFimISO,
            numeroOrcamento, numeroCliente, cliente,
            cnpj, uf, cidade, filial, codigoEmpresa,
            status, origem, condicaoPagamento, vendedor, tipoFrete,
            valorTotalMinimo, valorTotalMaximo,
            percentualDescontoMinimo, percentualDescontoMaximo,
            valorDescontoMinimo, valorDescontoMaximo,
            markupMinimo, markupMaximo
        );

        return await ExecuteOrcamentoQuery(ORCAMENTO_VIEW, filters, agrupamento, metrica, limite, ordenarPorMaiorValor);
    }

    [Description("[MODULO: ORCAMENTO] Consulta de itens de orcamento. Use filtros vazios (\"\") quando nao houver filtro.")]
    public async Task<string> ConsultarItensOrcamento(
        [Description("Codigo do item/produto (CODIGOITEM). Vazio para ignorar.")] string codigoItem = "",
        [Description("Nome do item/produto. Aceita multiplos separados por '|'. Vazio para ignorar.")] string item = "",
        [Description("Unidade de medida. Vazio para ignorar.")] string unidadeMedida = "",
        [Description("Codigo da empresa (CODEMPRESA). Aceita multiplos separados por '|' ou ','. Vazio para ignorar.")] string codigoEmpresa = "",
        [Description("Numero do orcamento (ORCAMENTO). Aceita multiplos separados por '|' ou ','. Vazio para ignorar.")] string numeroOrcamento = "",
        [Description("Quantidade minima. Vazio para ignorar.")] string quantidadeMinima = "",
        [Description("Quantidade maxima. Vazio para ignorar.")] string quantidadeMaxima = "",
        [Description("Valor unitario bruto minimo. Vazio para ignorar.")] string valorUnitarioBrutoMinimo = "",
        [Description("Valor unitario bruto maximo. Vazio para ignorar.")] string valorUnitarioBrutoMaximo = "",
        [Description("Valor unitario liquido minimo. Vazio para ignorar.")] string valorUnitarioLiquidoMinimo = "",
        [Description("Valor unitario liquido maximo. Vazio para ignorar.")] string valorUnitarioLiquidoMaximo = "",
        [Description("Valor total minimo. Vazio para ignorar.")] string valorTotalMinimo = "",
        [Description("Valor total maximo. Vazio para ignorar.")] string valorTotalMaximo = "",
        [Description("Percentual de desconto minimo. Vazio para ignorar.")] string percentualDescontoMinimo = "",
        [Description("Percentual de desconto maximo. Vazio para ignorar.")] string percentualDescontoMaximo = "",
        [Description("Valor de desconto minimo. Vazio para ignorar.")] string valorDescontoMinimo = "",
        [Description("Valor de desconto maximo. Vazio para ignorar.")] string valorDescontoMaximo = "",
        [Description("Markup minimo. Vazio para ignorar.")] string markupMinimo = "",
        [Description("Markup maximo. Vazio para ignorar.")] string markupMaximo = "",
        [Description("Agrupar por: NENHUM, TOTAL, ITEM, CODIGOITEM, UNIDADEMEDIDA.")] string agrupamento = "NENHUM",
        [Description("Metrica para ordenar/agregar: VALORTOTAL, QUANTIDADE, VALORUNITARIOBRUTO, VALORUNITARIOLIQUIDO, VALORDESCONTO, VALORACRESCIMO, MARKUP, PERCENTUALDESCONTO, PERCENTUALACRESCIMO.")] string metrica = "VALORTOTAL",
        [Description("Limite maximo de registros para listagem (quando agrupamento=NENHUM). Use para top N.")] int limite = 0,
        [Description("Se verdadeiro, ordena por maior metrica/valor.")] bool ordenarPorMaiorValor = false
    )
    {
        var filters = new BudgetItemFilters(
            codigoItem, item, unidadeMedida, codigoEmpresa, numeroOrcamento,
            quantidadeMinima, quantidadeMaxima,
            valorUnitarioBrutoMinimo, valorUnitarioBrutoMaximo,
            valorUnitarioLiquidoMinimo, valorUnitarioLiquidoMaximo,
            valorTotalMinimo, valorTotalMaximo,
            percentualDescontoMinimo, percentualDescontoMaximo,
            valorDescontoMinimo, valorDescontoMaximo,
            markupMinimo, markupMaximo
        );

        return await ExecuteItemQuery(ORCAMENTO_ITEM_VIEW, filters, agrupamento, metrica, limite, ordenarPorMaiorValor);
    }

    [Description("[MODULO: ORCAMENTO] Consulta cruzada entre orcamentos e itens usando CODEMPRESA + ORCAMENTO.")]
    public async Task<string> ConsultarOrcamentosComItens(
        [Description("Data inicial de emissao (ISO 8601). Vazio para ignorar.")] string dataInicioISO = "",
        [Description("Data final de emissao (ISO 8601). Vazio para ignorar.")] string dataFimISO = "",
        [Description("Numero do orcamento. Aceita multiplos separados por '|' ou ','. Vazio para ignorar.")] string numeroOrcamento = "",
        [Description("Cliente ou nome fantasia. Aceita multiplos separados por '|'. Vazio para ignorar.")] string cliente = "",
        [Description("CPF/CNPJ (somente numeros). Vazio para ignorar.")] string cnpj = "",
        [Description("UF. Vazio para ignorar.")] string uf = "",
        [Description("Filial/empresa (nome). Vazio para ignorar.")] string filial = "",
        [Description("Codigo da empresa (CODEMPRESA). Aceita multiplos separados por '|' ou ','. Vazio para ignorar.")] string codigoEmpresa = "",
        [Description("Status do orcamento. Vazio para ignorar.")] string status = "",
        [Description("Vendedor. Vazio para ignorar.")] string vendedor = "",
        [Description("Codigo do item (CODIGOITEM). Vazio para ignorar.")] string codigoItem = "",
        [Description("Nome do item. Aceita multiplos separados por '|'. Vazio para ignorar.")] string item = "",
        [Description("Agrupar por: TOTAL, ORCAMENTO, CLIENTE, VENDEDOR, STATUS, ITEM, UF, MES, FILIAL. (Obs: ITEM e ORCAMENTO ja retornam CodigoEmpresa para evitar mistura entre empresas).")] string agrupamento = "TOTAL",
        [Description("Metrica: VALORTOTAL_ORCAMENTO, VALORTOTAL_ITEM, QUANTIDADE_ITEM.")] string metrica = "VALORTOTAL_ITEM",
        [Description("Limite maximo de registros (quando agrupamento!=TOTAL). Use para top N.")] int limite = 0,
        [Description("Se verdadeiro, ordena por maior metrica/valor.")] bool ordenarPorMaiorValor = true
    )
    {
        var filters = new BudgetCrossFilters(
            dataInicioISO, dataFimISO, numeroOrcamento, cliente, cnpj, uf, filial, codigoEmpresa, status, vendedor, codigoItem, item
        );

        return await ExecuteCrossQuery(filters, agrupamento, metrica, limite, ordenarPorMaiorValor);
    }

    private async Task<string> ExecuteOrcamentoQuery(string viewName, BudgetOrcamentoFilters f, string agrupamento, string metrica, int limite, bool ordenarPorMaiorValor)
    {
        var conditions = new List<string>();
        var parameters = new List<SqlParameter>();

        if (!string.IsNullOrWhiteSpace(f.DataInicioISO))
        {
            conditions.Add("EMISSAO >= @dI");
            parameters.Add(new SqlParameter("@dI", ParseDate(f.DataInicioISO)));
        }
        if (!string.IsNullOrWhiteSpace(f.DataFimISO))
        {
            conditions.Add("EMISSAO <= @dF");
            parameters.Add(new SqlParameter("@dF", ParseDate(f.DataFimISO)));
        }
        if (!string.IsNullOrWhiteSpace(f.NumeroOrcamento))
        {
            AddMultiEquals(conditions, parameters, "ORCAMENTO", "@orc", f.NumeroOrcamento);
        }
        if (!string.IsNullOrWhiteSpace(f.NumeroCliente))
        {
            conditions.Add("NUMEROCLIENTE = @ncli");
            parameters.Add(new SqlParameter("@ncli", f.NumeroCliente.Trim()));
        }
        if (!string.IsNullOrWhiteSpace(f.Cliente))
        {
            AddMultiLike(conditions, parameters, "CLIENTE", "NOMEFANTASIA", "@cli", f.Cliente);
        }
        if (!string.IsNullOrWhiteSpace(f.Cnpj))
        {
            conditions.Add("CPFCNPJ = @cnpj");
            parameters.Add(new SqlParameter("@cnpj", LimparNumeros(f.Cnpj)));
        }
        if (!string.IsNullOrWhiteSpace(f.Uf))
        {
            conditions.Add("UF = @uf");
            parameters.Add(new SqlParameter("@uf", f.Uf.Trim().ToUpperInvariant()));
        }
        if (!string.IsNullOrWhiteSpace(f.Cidade))
        {
            conditions.Add("UPPER(CIDADE) LIKE UPPER(@cid)");
            parameters.Add(new SqlParameter("@cid", $"%{f.Cidade.Trim()}%"));
        }
        if (!string.IsNullOrWhiteSpace(f.Filial))
        {
            conditions.Add("UPPER(EMPRESA) LIKE UPPER(@fil)");
            parameters.Add(new SqlParameter("@fil", $"%{f.Filial.Trim()}%"));
        }
        if (!string.IsNullOrWhiteSpace(f.CodigoEmpresa))
        {
            AddMultiEquals(conditions, parameters, "CODEMPRESA", "@emp", f.CodigoEmpresa);
        }
        if (!string.IsNullOrWhiteSpace(f.Status))
        {
            conditions.Add("UPPER(STATUS) LIKE UPPER(@st)");
            parameters.Add(new SqlParameter("@st", $"%{f.Status.Trim()}%"));
        }
        if (!string.IsNullOrWhiteSpace(f.Origem))
        {
            conditions.Add("UPPER(ORIGEM) LIKE UPPER(@org)");
            parameters.Add(new SqlParameter("@org", $"%{f.Origem.Trim()}%"));
        }
        if (!string.IsNullOrWhiteSpace(f.CondicaoPagamento))
        {
            conditions.Add("UPPER(CONDPAG) LIKE UPPER(@cp)");
            parameters.Add(new SqlParameter("@cp", $"%{f.CondicaoPagamento.Trim()}%"));
        }
        if (!string.IsNullOrWhiteSpace(f.Vendedor))
        {
            conditions.Add("UPPER(VENDEDOR) LIKE UPPER(@vend)");
            parameters.Add(new SqlParameter("@vend", $"%{f.Vendedor.Trim()}%"));
        }
        if (!string.IsNullOrWhiteSpace(f.TipoFrete))
        {
            conditions.Add("UPPER(TIPOFRETE) LIKE UPPER(@tfr)");
            parameters.Add(new SqlParameter("@tfr", $"%{f.TipoFrete.Trim()}%"));
        }

        AddDecimalRange(conditions, parameters, "VALORTOTAL", "@vt", f.ValorTotalMinimo, f.ValorTotalMaximo);
        AddDecimalRange(conditions, parameters, "PERCENTUALDESCONTO", "@pd", f.PercentualDescontoMinimo, f.PercentualDescontoMaximo);
        AddDecimalRange(conditions, parameters, "VALORDESCONTO", "@vd", f.ValorDescontoMinimo, f.ValorDescontoMaximo);
        AddDecimalRange(conditions, parameters, "MARKUP", "@mk", f.MarkupMinimo, f.MarkupMaximo);

        var where = conditions.Count > 0 ? " WHERE " + string.Join(" AND ", conditions) : "";

        var group = Normalize(agrupamento, "NENHUM");
        var metricCol = ResolveOrcamentoMetric(Normalize(metrica, "VALORTOTAL"));

        if (group.Equals("TOTAL", StringComparison.OrdinalIgnoreCase))
        {
            var sql = $"SELECT SUM({metricCol}) as ValorTotalGeral, COUNT(*) as QuantidadeTotal FROM {viewName}{where}";
            return await ExecuteQuery(sql, parameters.ToArray());
        }

        if (!group.Equals("NENHUM", StringComparison.OrdinalIgnoreCase))
        {
            return await ExecuteGroupedOrcamentos(viewName, where, parameters, group, metricCol, limite, ordenarPorMaiorValor);
        }

        var baseCols = "CODEMPRESA, EMPRESA, ORCAMENTO, CLIENTE, NOMEFANTASIA, CPFCNPJ, CIDADE, UF, NUMEROCLIENTE, STATUS, EMISSAO, ORIGEM, CONDPAG, VENDEDOR, MARKUP, TIPOFRETE, VALORFRETE, PERCENTUALDESCONTO, VALORDESCONTO, PERCENTUALACRESCIMO, VALORACRESCIMO, VALORTOTAL";
        return await ExecuteListOrExport($"{baseCols} FROM {viewName}{where}", parameters, "EMISSAO", metricCol, limite, ordenarPorMaiorValor, viewName);
    }

    private async Task<string> ExecuteGroupedOrcamentos(string viewName, string where, List<SqlParameter> parameters, string agrupamento, string metricCol, int limite, bool ordenarPorMaiorValor)
    {
        string sql;
        if (agrupamento.Equals("ANO", StringComparison.OrdinalIgnoreCase))
            sql = $"SELECT YEAR(EMISSAO) as Ano, SUM({metricCol}) as Total, COUNT(*) as Quantidade FROM {viewName}{where} GROUP BY YEAR(EMISSAO) ORDER BY Ano DESC";
        else if (agrupamento.Equals("MES", StringComparison.OrdinalIgnoreCase))
            sql = $"SELECT YEAR(EMISSAO) as Ano, MONTH(EMISSAO) as Mes, SUM({metricCol}) as Total, COUNT(*) as Quantidade FROM {viewName}{where} GROUP BY YEAR(EMISSAO), MONTH(EMISSAO) ORDER BY Ano DESC, Mes DESC";
        else
        {
            var groupCol = ResolveOrcamentoGroupColumn(agrupamento);
            var orderCol = "Total";
            var orderDir = ordenarPorMaiorValor ? "DESC" : "ASC";
            sql = $"SELECT {groupCol} as Grupo, SUM({metricCol}) as Total, COUNT(*) as Quantidade FROM {viewName}{where} GROUP BY {groupCol} ORDER BY {orderCol} {orderDir}";
        }

        if (limite > 0)
            sql = $"SELECT TOP {limite} * FROM ({sql}) X";

        return await ExecuteQuery(sql, parameters.ToArray());
    }

    private async Task<string> ExecuteItemQuery(string viewName, BudgetItemFilters f, string agrupamento, string metrica, int limite, bool ordenarPorMaiorValor)
    {
        var conditions = new List<string>();
        var parameters = new List<SqlParameter>();

        if (!string.IsNullOrWhiteSpace(f.CodigoItem))
        {
            conditions.Add("CODIGOITEM = @cod");
            parameters.Add(new SqlParameter("@cod", f.CodigoItem.Trim()));
        }
        if (!string.IsNullOrWhiteSpace(f.Item))
        {
            AddMultiLike(conditions, parameters, "ITEM", null, "@it", f.Item);
        }
        if (!string.IsNullOrWhiteSpace(f.UnidadeMedida))
        {
            conditions.Add("UPPER(UNIDADEMEDIDA) LIKE UPPER(@un)");
            parameters.Add(new SqlParameter("@un", $"%{f.UnidadeMedida.Trim()}%"));
        }
        if (!string.IsNullOrWhiteSpace(f.CodigoEmpresa))
        {
            AddMultiEquals(conditions, parameters, "CODEMPRESA", "@emp", f.CodigoEmpresa);
        }
        if (!string.IsNullOrWhiteSpace(f.NumeroOrcamento))
        {
            AddMultiEquals(conditions, parameters, "ORCAMENTO", "@orc", f.NumeroOrcamento);
        }

        AddDecimalRange(conditions, parameters, "QUANTIDADE", "@q", f.QuantidadeMinima, f.QuantidadeMaxima);
        AddDecimalRange(conditions, parameters, "VALORUNITARIOBRUTO", "@vub", f.ValorUnitarioBrutoMinimo, f.ValorUnitarioBrutoMaximo);
        AddDecimalRange(conditions, parameters, "VALORUNITARIOLIQUIDO", "@vul", f.ValorUnitarioLiquidoMinimo, f.ValorUnitarioLiquidoMaximo);
        AddDecimalRange(conditions, parameters, "VALORTOTAL", "@vt", f.ValorTotalMinimo, f.ValorTotalMaximo);
        AddDecimalRange(conditions, parameters, "PERCENTUALDESCONTO", "@pd", f.PercentualDescontoMinimo, f.PercentualDescontoMaximo);
        AddDecimalRange(conditions, parameters, "VALORDESCONTO", "@vd", f.ValorDescontoMinimo, f.ValorDescontoMaximo);
        AddDecimalRange(conditions, parameters, "MARKUP", "@mk", f.MarkupMinimo, f.MarkupMaximo);

        var where = conditions.Count > 0 ? " WHERE " + string.Join(" AND ", conditions) : "";

        var group = Normalize(agrupamento, "NENHUM");
        var metricCol = ResolveItemMetric(Normalize(metrica, "VALORTOTAL"));

        if (group.Equals("TOTAL", StringComparison.OrdinalIgnoreCase))
        {
            var sql = $"SELECT SUM({metricCol}) as ValorTotalGeral, COUNT(*) as QuantidadeTotal FROM {viewName}{where}";
            return await ExecuteQuery(sql, parameters.ToArray());
        }

        if (!group.Equals("NENHUM", StringComparison.OrdinalIgnoreCase))
        {
            var groupCol = ResolveItemGroupColumn(group);
            var orderDir = ordenarPorMaiorValor ? "DESC" : "ASC";
            var sql = $"SELECT {groupCol} as Grupo, SUM({metricCol}) as Total, COUNT(*) as Quantidade FROM {viewName}{where} GROUP BY {groupCol} ORDER BY Total {orderDir}";
            if (limite > 0)
                sql = $"SELECT TOP {limite} * FROM ({sql}) X";
            return await ExecuteQuery(sql, parameters.ToArray());
        }

        var baseCols = "CODEMPRESA, ORCAMENTO, CODIGOITEM, ITEM, QUANTIDADE, UNIDADEMEDIDA, MARKUP, VALORUNITARIOBRUTO, VALORUNITARIOLIQUIDO, PERCENTUALDESCONTO, VALORDESCONTO, PERCENTUALACRESCIMO, VALORACRESCIMO, VALORTOTAL";
        return await ExecuteListOrExport($"{baseCols} FROM {viewName}{where}", parameters, "VALORTOTAL", metricCol, limite, ordenarPorMaiorValor, viewName);
    }

    private async Task<string> ExecuteCrossQuery(BudgetCrossFilters f, string agrupamento, string metrica, int limite, bool ordenarPorMaiorValor)
    {
        var conditions = new List<string>();
        var parameters = new List<SqlParameter>();

        if (!string.IsNullOrWhiteSpace(f.DataInicioISO))
        {
            conditions.Add("O.EMISSAO >= @dI");
            parameters.Add(new SqlParameter("@dI", ParseDate(f.DataInicioISO)));
        }
        if (!string.IsNullOrWhiteSpace(f.DataFimISO))
        {
            conditions.Add("O.EMISSAO <= @dF");
            parameters.Add(new SqlParameter("@dF", ParseDate(f.DataFimISO)));
        }
        if (!string.IsNullOrWhiteSpace(f.NumeroOrcamento))
        {
            AddMultiEquals(conditions, parameters, "O.ORCAMENTO", "@orc", f.NumeroOrcamento);
        }
        if (!string.IsNullOrWhiteSpace(f.CodigoEmpresa))
        {
            AddMultiEquals(conditions, parameters, "O.CODEMPRESA", "@emp", f.CodigoEmpresa);
        }
        if (!string.IsNullOrWhiteSpace(f.Cliente))
        {
            AddMultiLike(conditions, parameters, "O.CLIENTE", "O.NOMEFANTASIA", "@cli", f.Cliente);
        }
        if (!string.IsNullOrWhiteSpace(f.Cnpj))
        {
            conditions.Add("O.CPFCNPJ = @cnpj");
            parameters.Add(new SqlParameter("@cnpj", LimparNumeros(f.Cnpj)));
        }
        if (!string.IsNullOrWhiteSpace(f.Uf))
        {
            conditions.Add("O.UF = @uf");
            parameters.Add(new SqlParameter("@uf", f.Uf.Trim().ToUpperInvariant()));
        }
        if (!string.IsNullOrWhiteSpace(f.Filial))
        {
            conditions.Add("UPPER(O.EMPRESA) LIKE UPPER(@fil)");
            parameters.Add(new SqlParameter("@fil", $"%{f.Filial.Trim()}%"));
        }
        if (!string.IsNullOrWhiteSpace(f.Status))
        {
            conditions.Add("UPPER(O.STATUS) LIKE UPPER(@st)");
            parameters.Add(new SqlParameter("@st", $"%{f.Status.Trim()}%"));
        }
        if (!string.IsNullOrWhiteSpace(f.Vendedor))
        {
            conditions.Add("UPPER(O.VENDEDOR) LIKE UPPER(@vend)");
            parameters.Add(new SqlParameter("@vend", $"%{f.Vendedor.Trim()}%"));
        }
        if (!string.IsNullOrWhiteSpace(f.CodigoItem))
        {
            conditions.Add("I.CODIGOITEM = @cod");
            parameters.Add(new SqlParameter("@cod", f.CodigoItem.Trim()));
        }
        if (!string.IsNullOrWhiteSpace(f.Item))
        {
            AddMultiLike(conditions, parameters, "I.ITEM", null, "@it", f.Item);
        }

        var where = conditions.Count > 0 ? " WHERE " + string.Join(" AND ", conditions) : "";

        var group = Normalize(agrupamento, "TOTAL");
        var metric = Normalize(metrica, "VALORTOTAL_ITEM");

        var metricExpr = metric switch
        {
            "VALORTOTAL_ORCAMENTO" => "O.VALORTOTAL",
            "VALORTOTAL_ITEM" => "I.VALORTOTAL",
            "QUANTIDADE_ITEM" => "I.QUANTIDADE",
            _ => "I.VALORTOTAL"
        };

        var from = $"FROM {ORCAMENTO_VIEW} O INNER JOIN {ORCAMENTO_ITEM_VIEW} I ON I.CODEMPRESA = O.CODEMPRESA AND I.ORCAMENTO = O.ORCAMENTO";

        if (group.Equals("TOTAL", StringComparison.OrdinalIgnoreCase))
        {
            var sql = $"SELECT SUM({metricExpr}) as ValorTotalGeral, COUNT(*) as QuantidadeTotal {from}{where}";
            return await ExecuteQuery(sql, parameters.ToArray());
        }

        var groupDef = ResolveCrossGroupDefinition(group);

        var orderDir = ordenarPorMaiorValor ? "DESC" : "ASC";
        var sqlGrouped = $"SELECT {groupDef.SelectSql}, SUM({metricExpr}) as Total, COUNT(*) as Quantidade {from}{where} GROUP BY {groupDef.GroupBySql} ORDER BY Total {orderDir}";
        if (limite > 0)
            sqlGrouped = $"SELECT TOP {limite} * FROM ({sqlGrouped}) X";
        return await ExecuteQuery(sqlGrouped, parameters.ToArray());
    }

    private async Task<string> ExecuteListOrExport(string selectFrom, List<SqlParameter> parameters, string defaultSortCol, string metricCol, int limite, bool ordenarPorMaiorValor, string viewName)
    {
        var whereIndex = selectFrom.IndexOf(" FROM ", StringComparison.OrdinalIgnoreCase);
        if (whereIndex < 0)
            return await ExecuteQuery("SELECT " + selectFrom, parameters.ToArray());

        var fullSql = "SELECT " + selectFrom;
        var countSql = $"SELECT COUNT(*) AS Quantidade, SUM({metricCol}) AS ValorTotal FROM {selectFrom.Substring(whereIndex + 6)}";

        var (totalReal, valorTotal) = await ExecuteCountAndSum(countSql, parameters.ToArray());
        AggregateTotalLinhas += totalReal;
        AggregateValorTotal += valorTotal;

        var sortCol = ordenarPorMaiorValor ? metricCol : defaultSortCol;
        var sortOrder = ordenarPorMaiorValor ? "DESC" : "ASC";
        var orderByClause = $" ORDER BY {sortCol} {sortOrder}";

        if (limite > 0)
        {
            var limited = fullSql.Replace("SELECT ", $"SELECT TOP {limite} ");
            return await ExecuteListQueryInline(limited + orderByClause, countSql, parameters.ToArray(), metricCol);
        }

        if (totalReal <= EXPORT_THRESHOLD)
            return await ExecuteListQueryInline(fullSql + orderByClause, countSql, parameters.ToArray(), metricCol);

        return await ExecuteExportToCache(fullSql + orderByClause, totalReal, valorTotal, parameters.ToArray(), viewName);
    }

    private static string ResolveOrcamentoMetric(string metrica) => metrica.ToUpperInvariant() switch
    {
        "VALORFRETE" => "VALORFRETE",
        "VALORDESCONTO" => "VALORDESCONTO",
        "VALORACRESCIMO" => "VALORACRESCIMO",
        "MARKUP" => "MARKUP",
        "PERCENTUALDESCONTO" => "PERCENTUALDESCONTO",
        "PERCENTUALACRESCIMO" => "PERCENTUALACRESCIMO",
        "QUANTIDADE" => "1",
        _ => "VALORTOTAL"
    };

    private static string ResolveOrcamentoGroupColumn(string agrupamento) => agrupamento.ToUpperInvariant() switch
    {
        "CLIENTE" => "CLIENTE",
        "NOMEFANTASIA" => "NOMEFANTASIA",
        "FILIAL" => "EMPRESA",
        "CODEMPRESA" => "CODEMPRESA",
        "STATUS" => "STATUS",
        "ORIGEM" => "ORIGEM",
        "CONDPAG" => "CONDPAG",
        "VENDEDOR" => "VENDEDOR",
        "TIPOFRETE" => "TIPOFRETE",
        "CIDADE" => "CIDADE",
        "UF" => "UF",
        _ => "CLIENTE"
    };

    private static string ResolveItemMetric(string metrica) => metrica.ToUpperInvariant() switch
    {
        "QUANTIDADE" => "QUANTIDADE",
        "VALORUNITARIOBRUTO" => "VALORUNITARIOBRUTO",
        "VALORUNITARIOLIQUIDO" => "VALORUNITARIOLIQUIDO",
        "VALORDESCONTO" => "VALORDESCONTO",
        "VALORACRESCIMO" => "VALORACRESCIMO",
        "MARKUP" => "MARKUP",
        "PERCENTUALDESCONTO" => "PERCENTUALDESCONTO",
        "PERCENTUALACRESCIMO" => "PERCENTUALACRESCIMO",
        _ => "VALORTOTAL"
    };

    private static string ResolveItemGroupColumn(string agrupamento) => agrupamento.ToUpperInvariant() switch
    {
        "ITEM" => "ITEM",
        "CODIGOITEM" => "CODIGOITEM",
        "UNIDADEMEDIDA" => "UNIDADEMEDIDA",
        _ => "ITEM"
    };

    private static void AddMultiLike(List<string> conditions, List<SqlParameter> parameters, string col1, string? col2, string paramPrefix, string input)
    {
        var terms = SplitTerms(input);
        if (terms.Count == 0) return;

        if (terms.Count == 1)
        {
            var p = paramPrefix;
            conditions.Add(col2 == null
                ? $"(UPPER({col1}) LIKE UPPER({p}))"
                : $"(UPPER({col1}) LIKE UPPER({p}) OR UPPER({col2}) LIKE UPPER({p}))");
            parameters.Add(new SqlParameter(p, $"%{terms[0]}%"));
            return;
        }

        var orParts = new List<string>();
        for (int i = 0; i < terms.Count; i++)
        {
            var p = $"{paramPrefix}{i}";
            orParts.Add(col2 == null
                ? $"(UPPER({col1}) LIKE UPPER({p}))"
                : $"(UPPER({col1}) LIKE UPPER({p}) OR UPPER({col2}) LIKE UPPER({p}))");
            parameters.Add(new SqlParameter(p, $"%{terms[i]}%"));
        }
        conditions.Add("(" + string.Join(" OR ", orParts) + ")");
    }

    private static void AddMultiEquals(List<string> conditions, List<SqlParameter> parameters, string column, string paramPrefix, string input)
    {
        var values = SplitListValues(input);
        if (values.Count == 0) return;

        if (values.Count == 1)
        {
            conditions.Add($"{column} = {paramPrefix}");
            parameters.Add(new SqlParameter(paramPrefix, values[0]));
            return;
        }

        var inParts = new List<string>();
        for (int i = 0; i < values.Count; i++)
        {
            var p = $"{paramPrefix}{i}";
            inParts.Add(p);
            parameters.Add(new SqlParameter(p, values[i]));
        }

        conditions.Add($"{column} IN ({string.Join(", ", inParts)})");
    }

    private static void AddDecimalRange(List<string> conditions, List<SqlParameter> parameters, string column, string paramPrefix, string minRaw, string maxRaw)
    {
        if (TryParseDecimal(minRaw, out var min))
        {
            conditions.Add($"{column} >= {paramPrefix}Min");
            parameters.Add(new SqlParameter($"{paramPrefix}Min", min));
        }
        if (TryParseDecimal(maxRaw, out var max))
        {
            conditions.Add($"{column} <= {paramPrefix}Max");
            parameters.Add(new SqlParameter($"{paramPrefix}Max", max));
        }
    }

    private static bool TryParseDecimal(string raw, out decimal value)
    {
        value = 0;
        if (string.IsNullOrWhiteSpace(raw)) return false;
        var cleaned = raw.Trim().Replace(".", "").Replace(",", ".");
        return decimal.TryParse(cleaned, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out value);
    }

    private static List<string> SplitTerms(string input) =>
        input.Split(new[] { '|', ';' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(p => p.Trim())
            .Where(p => !string.IsNullOrWhiteSpace(p))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

    private static List<string> SplitListValues(string input) =>
        input.Split(new[] { '|', ';', ',' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(p => p.Trim())
            .Where(p => !string.IsNullOrWhiteSpace(p))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

    private record CrossGroupDef(string SelectSql, string GroupBySql);

    private static CrossGroupDef ResolveCrossGroupDefinition(string group)
        => group.ToUpperInvariant() switch
        {
            // ORCAMENTO e ITEM sao chaves compostas (CODEMPRESA + ORCAMENTO) e podem colidir entre empresas.
            "ORCAMENTO" => new CrossGroupDef("O.CODEMPRESA as CodigoEmpresa, O.ORCAMENTO as Grupo", "O.CODEMPRESA, O.ORCAMENTO"),
            "ITEM" => new CrossGroupDef("O.CODEMPRESA as CodigoEmpresa, I.ITEM as Grupo", "O.CODEMPRESA, I.ITEM"),

            "CLIENTE" => new CrossGroupDef("O.CLIENTE as Grupo", "O.CLIENTE"),
            "VENDEDOR" => new CrossGroupDef("O.VENDEDOR as Grupo", "O.VENDEDOR"),
            "STATUS" => new CrossGroupDef("O.STATUS as Grupo", "O.STATUS"),
            "UF" => new CrossGroupDef("O.UF as Grupo", "O.UF"),
            "FILIAL" => new CrossGroupDef("O.EMPRESA as Grupo", "O.EMPRESA"),
            "MES" => new CrossGroupDef("FORMAT(O.EMISSAO, 'yyyy-MM') as Grupo", "FORMAT(O.EMISSAO, 'yyyy-MM')"),
            _ => new CrossGroupDef("O.CLIENTE as Grupo", "O.CLIENTE")
        };

    private static string Normalize(string? value, string fallback)
        => string.IsNullOrWhiteSpace(value) ? fallback : value.Trim();

    private static string ParseDate(string isoDate)
    {
        if (DateTime.TryParse(isoDate, out var dt)) return dt.ToString("yyyyMMdd");
        return isoDate;
    }

    private static string LimparNumeros(string input)
        => string.IsNullOrEmpty(input) ? "" : new string(input.Where(char.IsDigit).ToArray());

    private string BuildRunnableQuery(string queryText, SqlParameter[] parameters)
    {
        var finalQuery = queryText;
        foreach (var p in parameters.OrderByDescending(p => p.ParameterName.Length))
        {
            var pName = p.ParameterName.StartsWith("@") ? p.ParameterName : "@" + p.ParameterName;
            var val = p.Value == null || p.Value == DBNull.Value ? "NULL" : "'" + p.Value.ToString()!.Replace("'", "''") + "'";
            finalQuery = finalQuery.Replace(pName, val);
        }
        return finalQuery;
    }

    private async Task<string> ExecuteQuery(string queryText, SqlParameter[] parameters)
    {
        try
        {
            var runnableQuery = BuildRunnableQuery(queryText, parameters);
            Console.WriteLine($"[BudgetPlugin] EXECUTING QUERY: {runnableQuery}");
            ExecutedQueries.Add(runnableQuery);

            using var connection = await _connectionFactory.CreateConnectionAsync();
            await ((System.Data.Common.DbConnection)connection).OpenAsync();

            using var command = connection.CreateCommand();
            command.CommandText = queryText;
            foreach (var p in parameters)
            {
                var dbParam = command.CreateParameter();
                dbParam.ParameterName = p.ParameterName;
                dbParam.Value = p.Value;
                command.Parameters.Add(dbParam);
            }

            using var reader = await ((System.Data.Common.DbCommand)command).ExecuteReaderAsync();
            var results = new List<Dictionary<string, object>>();
            while (await reader.ReadAsync())
            {
                var row = new Dictionary<string, object>();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    row[reader.GetName(i)] = reader.GetValue(i) == DBNull.Value ? null! : reader.GetValue(i);
                }
                results.Add(row);
            }

            var payload = new
            {
                TotalEncontradoNestaBusca = results.Count,
                AlertaQuantidade = "Ok",
                Data = results
            };

            return JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = true });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[BudgetPlugin] FATAL QUERY ERROR: {ex.Message}");
            return $"{{\"error\": \"Database error: {ex.Message}\"}}";
        }
    }

    private async Task<(int total, decimal valorTotal)> ExecuteCountAndSum(string countSql, SqlParameter[] parameters)
    {
        try
        {
            var runnable = BuildRunnableQuery(countSql, parameters);
            ExecutedQueries.Add(runnable);
            Console.WriteLine($"[BudgetPlugin] COUNT+SUM: {runnable}");

            using var connection = await _connectionFactory.CreateConnectionAsync();
            await ((System.Data.Common.DbConnection)connection).OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = countSql;
            foreach (var p in parameters)
            {
                var dp = cmd.CreateParameter();
                dp.ParameterName = p.ParameterName;
                dp.Value = p.Value;
                cmd.Parameters.Add(dp);
            }

            using var reader = await ((System.Data.Common.DbCommand)cmd).ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                int total = reader["Quantidade"] == DBNull.Value ? 0 : Convert.ToInt32(reader["Quantidade"]);
                decimal valor = reader["ValorTotal"] == DBNull.Value ? 0 : Convert.ToDecimal(reader["ValorTotal"]);
                return (total, valor);
            }
            return (0, 0);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[BudgetPlugin] COUNT ERRO: {ex.Message}");
            return (0, 0);
        }
    }

    private async Task<string> ExecuteListQueryInline(string listQuery, string countQuery, SqlParameter[] parameters, string sumColumn)
    {
        try
        {
            var runnableList = BuildRunnableQuery(listQuery, parameters);
            Console.WriteLine($"[BudgetPlugin] LIST INLINE: {runnableList}");
            ExecutedQueries.Add(runnableList);

            using var connection = await _connectionFactory.CreateConnectionAsync();
            await ((System.Data.Common.DbConnection)connection).OpenAsync();

            int totalReal = 0; decimal valorTotal = 0;
            using (var countCmd = connection.CreateCommand())
            {
                countCmd.CommandText = countQuery;
                foreach (var p in parameters)
                {
                    var dp = countCmd.CreateParameter();
                    dp.ParameterName = p.ParameterName; dp.Value = p.Value;
                    countCmd.Parameters.Add(dp);
                }
                using var cr = await ((System.Data.Common.DbCommand)countCmd).ExecuteReaderAsync();
                if (await cr.ReadAsync())
                {
                    totalReal = cr["Quantidade"] == DBNull.Value ? 0 : Convert.ToInt32(cr["Quantidade"]);
                    valorTotal = cr["ValorTotal"] == DBNull.Value ? 0 : Convert.ToDecimal(cr["ValorTotal"]);
                }
            }

            var results = new List<Dictionary<string, object>>();
            using (var listCmd = connection.CreateCommand())
            {
                listCmd.CommandText = listQuery;
                foreach (var p in parameters)
                {
                    var dp = listCmd.CreateParameter();
                    dp.ParameterName = p.ParameterName; dp.Value = p.Value;
                    listCmd.Parameters.Add(dp);
                }
                using var reader = await ((System.Data.Common.DbCommand)listCmd).ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    var row = new Dictionary<string, object>();
                    for (int i = 0; i < reader.FieldCount; i++)
                        row[reader.GetName(i)] = reader.GetValue(i) == DBNull.Value ? null! : reader.GetValue(i);
                    results.Add(row);
                }
            }

            var payload = new
            {
                TotalDeDocumentosNoBanco = totalReal,
                ValorTotalConfirmado = valorTotal,
                ExibindoPrimeirosDocumentos = results.Count,
                AlertaParaIA = $"LISTAGEM COMPLETA: Todos os {totalReal} registros estao exibidos. Total confirmado: R$ {valorTotal:F2}. " +
                               $"EXIBA este valor como rodape da tabela: **Total: R$ {valorTotal:N2} ({totalReal} registros)**",
                Data = results
            };

            return JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = true });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[BudgetPlugin] INLINE ERRO: {ex.Message}");
            return $"{{\"error\": \"{ex.Message}\"}}";
        }
    }

    private async Task<string> ExecuteExportToCache(string fullQuery, int totalReal, decimal valorTotal, SqlParameter[] parameters, string viewName)
    {
        try
        {
            var runnable = BuildRunnableQuery(fullQuery, parameters);
            Console.WriteLine($"[BudgetPlugin] EXPORT FULL QUERY ({totalReal} rows): {runnable}");
            ExecutedQueries.Add(runnable);

            using var connection = await _connectionFactory.CreateConnectionAsync();
            await ((System.Data.Common.DbConnection)connection).OpenAsync();

            var results = new List<Dictionary<string, object>>();
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = fullQuery;
                foreach (var p in parameters)
                {
                    var dp = cmd.CreateParameter();
                    dp.ParameterName = p.ParameterName; dp.Value = p.Value;
                    cmd.Parameters.Add(dp);
                }
                using var reader = await ((System.Data.Common.DbCommand)cmd).ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    var row = new Dictionary<string, object>();
                    for (int i = 0; i < reader.FieldCount; i++)
                        row[reader.GetName(i)] = reader.GetValue(i) == DBNull.Value ? null! : reader.GetValue(i);
                    results.Add(row);
                }
            }

            var excelBytes = GerarExcel(results, viewName, totalReal, valorTotal);

            var exportId = Guid.NewGuid().ToString();
            _cache.Set($"export:{exportId}", excelBytes, TimeSpan.FromMinutes(30));

            LastExportId = exportId;
            LastExportTotalLinhas = totalReal;
            LastExportValorTotal = valorTotal;

            var rawDataJson = JsonSerializer.Serialize(results);
            _cache.Set($"export-data:{exportId}", rawDataJson, TimeSpan.FromMinutes(30));

            return JsonSerializer.Serialize(new
            {
                tipo = "EXPORT_PRONTO",
                exportId = exportId,
                totalLinhas = totalReal,
                valorTotalConfirmado = valorTotal,
                instrucaoParaIA =
                    $"Relatorio gerado com sucesso: {totalReal} registros, valor total R$ {valorTotal:N2}. " +
                    "Informe ao usuario quantos registros foram encontrados e o valor total. " +
                    "Nao mencione links, URLs, exportId nem instrucoes de download."
            }, new JsonSerializerOptions { WriteIndented = true });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[BudgetPlugin] EXPORT ERRO: {ex.Message}");
            return $"{{\"error\": \"{ex.Message}\"}}";
        }
    }

    private static byte[] GerarExcel(List<Dictionary<string, object>> rows, string viewName, int total, decimal valorTotal)
    {
        using var workbook = new XLWorkbook();
        var ws = workbook.Worksheets.Add("Relatorio");

        if (rows.Count == 0)
        {
            ws.Cell(1, 1).Value = "Nenhum registro encontrado.";
        }
        else
        {
            var columns = rows[0].Keys.ToList();
            for (int col = 0; col < columns.Count; col++)
            {
                var headerCell = ws.Cell(1, col + 1);
                headerCell.Value = columns[col];
                headerCell.Style.Font.Bold = true;
                headerCell.Style.Fill.BackgroundColor = XLColor.FromHtml("#1E293B");
                headerCell.Style.Font.FontColor = XLColor.White;
                headerCell.Style.Alignment.Horizontal = XLAlignmentHorizontalValues.Center;
            }

            for (int row = 0; row < rows.Count; row++)
            {
                for (int col = 0; col < columns.Count; col++)
                {
                    var val = rows[row].GetValueOrDefault(columns[col]);
                    var cell = ws.Cell(row + 2, col + 1);

                    if (val is DateTime dt) cell.Value = dt;
                    else if (val is decimal dec) cell.Value = dec;
                    else if (val is double dbl) cell.Value = dbl;
                    else if (val is int integer) cell.Value = integer;
                    else if (val is long lng) cell.Value = lng;
                    else cell.Value = val?.ToString() ?? "";

                    if (row % 2 == 1)
                        cell.Style.Fill.BackgroundColor = XLColor.FromHtml("#F8FAFC");
                }
            }

            int footerRow = rows.Count + 3;
            ws.Cell(footerRow, 1).Value = $"Total de Registros: {total}";
            ws.Cell(footerRow, 1).Style.Font.Bold = true;
            ws.Cell(footerRow + 1, 1).Value = $"Valor Total: R$ {valorTotal:N2}";
            ws.Cell(footerRow + 1, 1).Style.Font.Bold = true;

            ws.Columns().AdjustToContents();
        }

        using var stream = new MemoryStream();
        workbook.SaveAs(stream);
        return stream.ToArray();
    }

    private record BudgetOrcamentoFilters(
        string DataInicioISO, string DataFimISO,
        string NumeroOrcamento, string NumeroCliente, string Cliente,
        string Cnpj, string Uf, string Cidade, string Filial, string CodigoEmpresa,
        string Status, string Origem, string CondicaoPagamento, string Vendedor, string TipoFrete,
        string ValorTotalMinimo, string ValorTotalMaximo,
        string PercentualDescontoMinimo, string PercentualDescontoMaximo,
        string ValorDescontoMinimo, string ValorDescontoMaximo,
        string MarkupMinimo, string MarkupMaximo
    );

    private record BudgetItemFilters(
        string CodigoItem, string Item, string UnidadeMedida, string CodigoEmpresa, string NumeroOrcamento,
        string QuantidadeMinima, string QuantidadeMaxima,
        string ValorUnitarioBrutoMinimo, string ValorUnitarioBrutoMaximo,
        string ValorUnitarioLiquidoMinimo, string ValorUnitarioLiquidoMaximo,
        string ValorTotalMinimo, string ValorTotalMaximo,
        string PercentualDescontoMinimo, string PercentualDescontoMaximo,
        string ValorDescontoMinimo, string ValorDescontoMaximo,
        string MarkupMinimo, string MarkupMaximo
    );

    private record BudgetCrossFilters(
        string DataInicioISO, string DataFimISO, string NumeroOrcamento, string Cliente, string Cnpj, string Uf, string Filial, string CodigoEmpresa,
        string Status, string Vendedor, string CodigoItem, string Item
    );
}
