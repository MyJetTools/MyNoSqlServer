using System.Threading.Tasks;
using Autofac;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MyDependencies;
using MyNoSqlServer.Api.Hubs;
using MyNoSqlServer.AzureStorage;
using MyNoSqlServer.Domains;


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

        public SettingsModel _settings;

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {

            services.AddApplicationInsightsTelemetry(Configuration);

            services.AddControllers();

            services.AddSignalR();

            services.AddSwaggerDocument(o => { o.Title = "MyNoSqlServer"; });

            _settings = SettingsLoader.LoadSettings();
            
            IoC.BindDomainsServices();
            IoC.BindAzureStorage(_settings.BackupAzureConnectString);
            IoC.BindApiServices();
            
            
            ServiceLocator.Init(IoC);

        }

        public void ConfigureContainer(ContainerBuilder builder)
        {

            /* FOR EXAMPLE
             
            register instance 
             
            builder
                .RegisterInstance(new AzureBlobSnapshotStorage(_settings.BackupAzureConnectString))
                .As<ISnapshotStorage>()
                .SingleInstance();

            register class
            link to class you can claim in constructor
            if class has dependency then all dependencies resolve from container

            builder
                .RegisterType<AzureBlobSnapshotStorage>()
                .As<ISnapshotStorage>()
                .SingleInstance();

            */
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
            });
            
            ServiceLocator.Start();
        }

        private void OnShutdown()
        {
            ServiceLocator.GlobalVariables.IsShuttingDown = true;
            Task.Delay(500).Wait();

            ServiceLocator.SnapshotSaverEngine.Stop();
        }
    }
}
 
 