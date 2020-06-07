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


        public static IActionResult TableNotFound(this Controller ctx)
        {
            return ctx.NotFound($"Table not found");
        }
        
        
        public static IActionResult ApplicationIsShuttingDown(this Controller ctx)
        {
            return ctx.NotFound("Application is Shutting Down");
        }
        
        public static IActionResult RecordIsNotFound(this Controller ctx)
        {
            return ctx.NotFound("Record is not found");
        }


        public static IActionResult PartitionKeyIsNull(this Controller ctx)
        {
            return ctx.NotFound("Please specify PartitionKey");
        }

        public static IActionResult RowKeyIsNull(this Controller ctx)
        {
            return ctx.NotFound("Please specify RowKey");
        }

        public static IActionResult RowNotFound(this Controller ctx)
        {
            return ctx.NotFound("Row not Found");
        }



    }

}
