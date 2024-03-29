using System;
using Microsoft.AspNetCore.Builder;

namespace MyNoSqlServer.Api.Middlewares
{
    public static class ErrorMiddleware
    {

        public static void UseErrorMiddleware(this IApplicationBuilder app)
        {
            app.Use(async (ctx, next) =>
            {

                try
                {
                    await next();
                }
                catch (Exception e)
                {
                    ServiceLocator.Logger.WriteError(ctx.Request.Path.ToString(), e);
                    throw;
                }

            });
        }
        
    }
}