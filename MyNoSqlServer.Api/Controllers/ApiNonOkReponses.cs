using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;

namespace MyNoSqlServer.Api.Controllers
{

    public static class ApiNonOkResponses
    {
        
        public static IActionResult ResponseOk(this Controller ctx)
        {
            return ctx.Content("OK");
        }

        public static ValueTask<IActionResult> ResponseWithActionAsync(this IActionResult actionResult, Func<Task> callback)
        {
            var theTask = callback().ContinueWith(task => actionResult);
            return new ValueTask<IActionResult>(theTask);
        }

        public static IActionResult ResponseConflict(this Controller ctx, string message)
        {
            return ctx.Conflict(message);
        }

        public static IActionResult TableNameIsNull(this Controller ctx)
        {
            return ctx.NotFound("Please specify table name");
        }

        public static IActionResult QueryIsNull(this Controller ctx)
        {
            return ctx.NotFound("Please specify query as body json field");
        }


        public static IActionResult TableNotFound(this Controller ctx, string tableName)
        {
            return ctx.NotFound($"Table {tableName} not found");
        }

        public static IActionResult PartitionKeyIsNull(this Controller ctx)
        {
            return ctx.NotFound("Please specify PartitionKey");
        }

        public static IActionResult RowKeyIsNull(this Controller ctx)
        {
            return ctx.NotFound("Please specify RowKey");
        }

        public static IActionResult RowNotFound(this Controller ctx, string tableName, string partitionKey, string rowKey)
        {
            return ctx.StatusCode(204);
        }



    }

}
