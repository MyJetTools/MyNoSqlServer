using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using MyNoSqlServer.Abstractions;

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

        public static IActionResult ResponseConflict(this Controller ctx, OperationResult operationResult)
        {
            return ctx.Conflict(((int)operationResult).ToString());
        }




    }

}
