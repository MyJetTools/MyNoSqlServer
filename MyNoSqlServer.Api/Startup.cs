using System;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MyNoSqlServer.Api.Grpc;
using MyNoSqlServer.Api.Middlewares;
using MyNoSqlServer.Domains;
using Prometheus;
using ProtoBuf.Grpc.Server;


namespace MyNoSqlServer.Api
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        public static SettingsModel Settings { get; private set; }

        private IServiceCollection _services;

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {

            _services = services;
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
            services.AddCodeFirstGrpc();
            

            services.AddApplicationInsightsTelemetry(Configuration);

            services.AddControllers();

            services.AddSignalR();

            services.AddSwaggerDocument(o => { o.Title = "MyNoSqlServer"; });

            Settings = SettingsLoader.LoadSettings();
            
            services.AddSingleton<IMyNoSqlNodePersistenceSettings>(Settings);
            services.AddSingleton<ISettingsLocation>(Settings);
            
            
            services.BindDomainsServices();

   
            
            services.BindDataReadersTcpServices();

            if (Settings.IsNode())
                services.BindAsNodeServices(Settings);
            else
                services.BindAsRootNodeServices(Settings);

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

            app.UseErrorMiddleware();
            app.UseRouting();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapGrpcService<MyNoSqlGrpcService>();
                endpoints.MapGrpcService<PersistenceNodeGrpcService>();
                endpoints.MapGrpcService<NodeSyncGrpcService>();
                endpoints.MapControllers();
                endpoints.MapMetrics();
                
            });

            var sp = _services.BuildServiceProvider();
            ServiceLocator.Init(sp);
            
            ServiceLocator.Start(sp, Settings);
        }

        private void OnShutdown()
        {
            ServiceLocator.Stop();
        }
    }
}
 
 