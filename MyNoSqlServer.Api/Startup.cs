using System;
using System.Threading.Tasks;
using Autofac;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MyDependencies;
using MyNoSqlServer.Api.Hubs;
using MyNoSqlServer.Api.Models;
using MyNoSqlServer.AzureStorage;
using MyNoSqlServer.Common;
using MyNoSqlServer.Domains;
using MyTcpSockets;
using Prometheus;


namespace MyNoSqlServer.Api
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }
        
        public static MyIoc IoC = new MyIoc();

        public static SettingsModel Settings { get; private set; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {

            services.AddApplicationInsightsTelemetry(Configuration);

            services.AddControllers();

            services.AddSignalR();

            services.AddSwaggerDocument(o => { o.Title = "MyNoSqlServer"; });

            Settings = SettingsLoader.LoadSettings();
            
            IoC.BindDomainsServices();
            IoC.BindAzureStorage(Settings.BackupAzureConnectString);
            IoC.BindApiServices();

            MyNoSqlServerMemory.AllocateByteArray = size => GC.AllocateUninitializedArray<byte>(size);
            SocketMemoryUtils.AllocateByteArray = size => GC.AllocateUninitializedArray<byte>(size);
            
            ServiceLocator.Init(IoC);

        }

        public void ConfigureContainer(ContainerBuilder builder)
        {
            builder.RegisterModule(new ServiceModule(Settings.BackupAzureConnectString));
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostApplicationLifetime applicationLifetime)
        {

            // Enable middleware to serve generated Swagger as a JSON endpoint.
//                  app.UseSwagger();

            // Enable middleware to serve swagger-ui (HTML, JS, CSS, etc.), 
            // specifying the Swagger JSON endpoint.
               

            //app.UseHttpsRedirection();


            applicationLifetime.ApplicationStopping.Register(OnShutdown);

            app.Use((context, next) =>
            {
                if (context.Request.Headers.ContainsKey("X-Forwarded-Proto"))
                    context.Request.Scheme = context.Request.Headers["X-Forwarded-Proto"];
                return next();
            });

            //  app.UseForwardedHeaders();

            
            app.UseStaticFiles();

            app.UseOpenApi();
            app.UseSwaggerUi3();

            app.UseRouting();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
                endpoints.MapHub<ChangesHub>("/changes");
                endpoints.MapMetrics();
            });
            
            ServiceLocator.Start();
        }

        private void OnShutdown()
        {
            ServiceLocator.GlobalVariables.IsShuttingDown = true;
            Task.Delay(500).Wait();
        }
    }
}
 
 