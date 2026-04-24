using System.Data;

namespace IT4You.Application.FinanceAnalytics.Interfaces;

/// <summary>
/// Factory que cria conexões ao banco ERP do Tenant logado na requisição atual.
/// A connection string é resolvida dinamicamente com base nos dados cadastrados
/// no Tenant (DbIp, DbName, DbType, DbUser, DbPassword).
/// </summary>
public interface IErpConnectionFactory
{
    /// <summary>
    /// Retorna a connection string montada para o tenant do usuário logado.
    /// </summary>
    Task<string> GetConnectionStringAsync();

    /// <summary>
    /// Cria e retorna uma IDbConnection (SqlConnection ou OracleConnection)
    /// para o banco ERP do tenant do usuário logado. A conexão ainda não foi aberta.
    /// </summary>
    Task<IDbConnection> CreateConnectionAsync();
}
