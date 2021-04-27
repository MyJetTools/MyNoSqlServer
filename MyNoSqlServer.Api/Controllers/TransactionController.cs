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
        public StartTransactionResponse Start()
        {

            var transaction = ServiceLocator.PostTransactionsList.StartTransaction();

            return new StartTransactionResponse
            {
                TransactionId = transaction.Id
            };
        }

        [HttpPost("/Transaction/Append")]
        public async ValueTask<IActionResult> Append([Required] string transactionId, [Required] string tableName)
        {
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
            var transaction = ServiceLocator.PostTransactionsList.TryDelete(transactionId);

            var (tableName, transactions) = transaction.GetNextTransactionsToExecute();

            while (tableName != null)
            {

                var table = ServiceLocator.DbInstance.TryGetTable(tableName);

                if (table != null)
                    await ServiceLocator.DbOperations.ApplyTransactionsAsync(table, transactions);

                (tableName, transactions) = transaction.GetNextTransactionsToExecute();
            }

            return this.ResponseOk();
        }

    }
}