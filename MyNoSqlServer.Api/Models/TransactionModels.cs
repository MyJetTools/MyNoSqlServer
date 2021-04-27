using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using MyNoSqlServer.Api.Services;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Json;
using MyNoSqlServer.Domains.Transactions;

namespace MyNoSqlServer.Api.Models
{
    public class StartTransactionResponse
    {
        public string TransactionId { get; set; }
    }

     
    

 
}