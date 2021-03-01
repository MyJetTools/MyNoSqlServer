using Autofac;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MyNoSqlServer.Api.Hubs;
using MyNoSqlServer.Api.Models;
using MyNoSqlServer.AzureStorage;
using MyNoSqlServer.Domains;
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

        public SettingsModel _settings;

        private IServiceCollection _services;

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {

            _services = services;

            services.AddApplicationInsightsTelemetry(Configuration);

            services.AddControllers();

            services.AddSignalR();

            services.AddSwaggerDocument(o => { o.Title = "MyNoSqlServer"; });

            _settings = SettingsLoader.LoadSettings();
            
            services.BindDomainsServices();
            services.BindAzureStorage(_settings.BackupAzureConnectString);
            services.BindApiServices();

        }

        public void ConfigureContainer(ContainerBuilder builder)
        {
            builder.RegisterModule(new ServiceModule(_settings.BackupAzureConnectString));
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

            var sp = _services.BuildServiceProvider();
            ServiceLocator.Init(sp);
            
            ServiceLocator.Start();
        }

        private void OnShutdown()
        {
            ServiceLocator.Stop();
        }
    }
}
 
 