using System;
using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.Api.Models;
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
        public async ValueTask<IActionResult> Append([Required] string transactionId, [Required] string tableName)
        {
            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return shutDown;

            var table = ServiceLocator.DbInstance.TryGetTable(tableName);

            if (table == null)
                return this.GetResult(OperationResult.TableNotFound);

            var transaction = ServiceLocator.PostTransactionsList.TryGet(transactionId);

            if (transaction == null)
                return NotFound("Transaction not found with Id: " + transactionId);


            var body = await Request.BodyAsIMemoryAsync();

            var transactions = DbTransactionsJsonDeserializer.GetTransactions(body);

            transaction.PostTransactions(tableName, transactions);

            return this.ResponseOk();
        }

        [HttpPost("/Transaction/Commit")]
        public async Task<IActionResult> Commit([Required] string transactionId)
        {

            var shutDown = this.CheckOnShuttingDown();
            if (shutDown != null)
                return shutDown;

            var transaction = ServiceLocator.PostTransactionsList.TryDelete(transactionId);

            var (tableName, transactions) = transaction.GetNextTransactionsToExecute();

            while (tableName != null)
            {

                Console.WriteLine("TableName: "+tableName);
                
                var table = ServiceLocator.DbInstance.TryGetTable(tableName);

                if (table != null)
                    await ServiceLocator.DbOperations.ApplyTransactionsAsync(table, transactions);

                (tableName, transactions) = transaction.GetNextTransactionsToExecute();
            }

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