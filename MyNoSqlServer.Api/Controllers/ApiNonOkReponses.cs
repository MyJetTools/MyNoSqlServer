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

        public static IActionResult ResponseConflict(this Controller ctx, OperationResult operationResult)
        {
            return ctx.Conflict(((int)operationResult).ToString());
        }

    }

}
