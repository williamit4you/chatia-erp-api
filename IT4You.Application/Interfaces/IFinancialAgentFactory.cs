using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Agents.AI;

namespace IT4You.Application.Interfaces
{
    public interface IFinancialAgentFactory
    {
        Task<AIAgent> CreateAgentAsync(string iaToken, bool hasPayableAccess, bool hasReceivableAccess);
    }
}
