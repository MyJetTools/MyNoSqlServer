using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Api.Models;
using MyNoSqlServer.Domains.Db.Tables;
using MyNoSqlServer.Domains.Transactions;

namespace MyNoSqlServer.Api.Controllers
{
    [ApiController]
    public class TransactionController : Controller
    {

        [HttpPost("/Transaction/Start")]
        public IActionResult Start()
        {
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return shutDown;

            var transaction = ServiceLocator.PostTransactionsList.StartTransaction();

            var result = new StartTransactionResponse
            {
                TransactionId = transaction.Id
            };

            return Json(result);
        }

        [HttpPost("/Transaction/Append")]
        public async ValueTask<IActionResult> Append([Required] string transactionId)
        {
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return shutDown;

         

            var transaction = ServiceLocator.PostTransactionsList.TryGet(transactionId);

            if (transaction == null)
                return NotFound("Transaction not found with Id: " + transactionId);


            var body = await Request.BodyAsIMemoryAsync();

            var transactions = DbTransactionsJsonDeserializer.GetTransactions(body).ToList();
            
            var tables = new Dictionary<string, DbTable>();

            foreach (var dbTransactionCommand in transactions)
            {
                if (tables.ContainsKey(dbTransactionCommand.TableName))
                    continue;

                var table = ServiceLocator.DbInstance.TryGetTable(dbTransactionCommand.TableName);

                if (table == null)
                    return this.GetResult(OperationResult.TableNotFound);
                
                tables.Add(table.Name, table);
            }

            transaction.PostTransactions(tables.Values, transactions);

            return this.ResponseOk();
        }

        [HttpPost("/Transaction/Commit")]
        public async Task<IActionResult> Commit([Required] string transactionId)
        {

            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return shutDown;

            var transaction = ServiceLocator.PostTransactionsList.TryDelete(transactionId);
            
            await ServiceLocator.DbOperations.ApplyTransactionsAsync(transaction.Tables, transaction.GetTransactionsToExecute());

            return this.ResponseOk();
        }

        [HttpPost("/Transaction/Cancel")]
        public IActionResult Cancel([Required] string transactionId)
        {
            ServiceLocator.PostTransactionsList.TryDelete(transactionId);
            return this.ResponseOk();
        }

    }
}