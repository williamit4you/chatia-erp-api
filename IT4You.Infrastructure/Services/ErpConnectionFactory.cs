using System.Data;
using IT4You.Application.Data;
using IT4You.Application.FinanceAnalytics.Interfaces;
using IT4You.Application.Helpers;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace IT4You.Infrastructure.Services;

/// <summary>
/// Implementação de <see cref="IErpConnectionFactory"/> que resolve a conexão ERP
/// dinamicamente a partir do cadastro do Tenant do usuário logado no request atual.
/// Suporta SQL Server e Oracle. Registro recomendado: Scoped.
/// </summary>
public class ErpConnectionFactory : IErpConnectionFactory
{
    private readonly IHttpContextAccessor _httpContextAccessor;
    private readonly AppDbContext _dbContext;
    private readonly ILogger<ErpConnectionFactory> _logger;

    public ErpConnectionFactory(
        IHttpContextAccessor httpContextAccessor,
        AppDbContext dbContext,
        ILogger<ErpConnectionFactory> logger)
    {
        _httpContextAccessor = httpContextAccessor;
        _dbContext = dbContext;
        _logger = logger;
    }

    public async Task<string> GetConnectionStringAsync()
    {
        // Lê o tenantId do JWT (claim "tenantId" emitido pelo AuthController)
        var tenantId = _httpContextAccessor.HttpContext?.User
            .FindFirst("tenantId")?.Value;

        if (string.IsNullOrEmpty(tenantId))
        {
            _logger.LogWarning("[ErpConnectionFactory] TenantId ausente no token. Requisição sem autenticação ou usuário é SUPER_ADMIN sem tenant.");
            throw new UnauthorizedAccessException(
                "Não foi possível identificar a empresa do usuário logado. " +
                "Verifique se o token de autenticação é válido.");
        }

        var tenant = await _dbContext.Tenants.FindAsync(tenantId);
        if (tenant == null)
        {
            _logger.LogError("[ErpConnectionFactory] Tenant '{TenantId}' não encontrado no banco.", tenantId);
            throw new InvalidOperationException($"Empresa não encontrada (id: {tenantId}).");
        }

        if (string.IsNullOrWhiteSpace(tenant.DbIp) || string.IsNullOrWhiteSpace(tenant.DbName))
        {
            _logger.LogWarning("[ErpConnectionFactory] Tenant '{TenantId}' ({Name}) sem configuração de banco ERP.", tenantId, tenant.Name);
            throw new InvalidOperationException(
                $"A empresa '{tenant.Name}' ainda não possui banco de dados configurado. " +
                "Acesse Configurações da Empresa e preencha os dados de conexão ERP.");
        }

        // Decripta a senha (EncryptionHelper trata legado não criptografado automaticamente)
        var password = EncryptionHelper.Decrypt(tenant.DbPassword ?? "");
        var dbType = (tenant.DbType ?? "SQLSERVER").Trim().ToUpperInvariant();

        _logger.LogInformation("[ErpConnectionFactory] Montando conexão para Tenant '{TenantId}' ({Name}), DbType={DbType}, Server={DbIp}, Database={DbName}",
            tenantId, tenant.Name, dbType, tenant.DbIp, tenant.DbName);

        return dbType switch
        {
            "ORACLE" => BuildOracleConnectionString(tenant.DbIp, tenant.DbName, tenant.DbUser ?? "", password),
            _        => BuildSqlServerConnectionString(tenant.DbIp, tenant.DbName, tenant.DbUser ?? "", password)
        };
    }

    public async Task<IDbConnection> CreateConnectionAsync()
    {
        // Necessário ler DbType novamente para saber qual driver usar
        var tenantId = _httpContextAccessor.HttpContext?.User
            .FindFirst("tenantId")?.Value;

        string dbType = "SQLSERVER";
        if (!string.IsNullOrEmpty(tenantId))
        {
            var tenant = await _dbContext.Tenants.FindAsync(tenantId);
            if (tenant != null)
                dbType = (tenant.DbType ?? "SQLSERVER").Trim().ToUpperInvariant();
        }

        var connStr = await GetConnectionStringAsync();

        if (dbType == "ORACLE")
        {
            // Suporte a Oracle via Oracle.ManagedDataAccess.Core
            // Usando reflection para não criar dependência hard em Oracle no projeto Application
            try
            {
                var oracleType = Type.GetType("Oracle.ManagedDataAccess.Client.OracleConnection, Oracle.ManagedDataAccess");
                if (oracleType != null)
                {
                    var conn = (IDbConnection)Activator.CreateInstance(oracleType, connStr)!;
                    return conn;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "[ErpConnectionFactory] Falha ao criar OracleConnection via reflection.");
            }

            throw new InvalidOperationException(
                "Driver Oracle não encontrado. Verifique se o pacote 'Oracle.ManagedDataAccess.Core' está instalado.");
        }

        // Padrão: SQL Server
        return new SqlConnection(connStr);
    }

    // ------------------------------------------------------------------
    // Builders de connection string
    // ------------------------------------------------------------------

    private static string BuildSqlServerConnectionString(string server, string database, string user, string password)
    {
        // Suporta porta personalizada: se DbIp vier como "192.168.1.10,1433"
        return $"Server={server};Database={database};User Id={user};Password={password};TrustServerCertificate=True;Connection Timeout=30;";
    }

    private static string BuildOracleConnectionString(string host, string serviceName, string user, string password)
    {
        // Formato TNS simplificado: host/serviceName (ex: "192.168.1.10/ORCL")
        // Se DbIp já vier no formato host:port, o Oracle Driver aceita
        return $"Data Source={host}/{serviceName};User Id={user};Password={password};";
    }
}
