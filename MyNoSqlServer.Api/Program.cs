using System;
using System.Net;
using Autofac.Extensions.DependencyInjection;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MyNoSqlServer.Api
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args)
                .Build()
                .Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    
                    webBuilder.ConfigureKestrel(options =>
                    {
                        options.Listen(IPAddress.Any, 5123,
                            o => o.Protocols = HttpProtocols.Http1);

                        options.Listen(IPAddress.Any, 5124,
                            o => o.Protocols = HttpProtocols.Http2);
                    });
                    
                    webBuilder
                        .UseStartup<Startup>()
                        .ConfigureLogging((context, logging) =>
                        {
                            if (Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") == "Production")
                                logging.ClearProviders();
                        });

                })
                .UseServiceProviderFactory(new AutofacServiceProviderFactory());
        
    }
}