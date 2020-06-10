using System;
using System.IO;
using Microsoft.Extensions.Configuration;
using MyNoSqlServer.Common;

namespace MyNoSqlServer.Api
{
    public class SettingsModel
    {
        public string BackupAzureConnectString { get; set; }
    }

    public static class SettingsLoader
    {
        public static SettingsModel LoadSettings()
        {
            try
            {
                var configBuilder = new ConfigurationBuilder();

                var homeFolder = Environment.GetEnvironmentVariable("HOME");
                if (!string.IsNullOrEmpty(homeFolder) && File.Exists(Path.Combine(homeFolder, ".mynosqlserver")))
                {
                    FileStream fileStream = new FileStream(Path.Combine(homeFolder, ".mynosqlserver"), FileMode.Open);
                    configBuilder.AddJsonStream(fileStream);
                }

                homeFolder = Environment.GetEnvironmentVariable("HOMEDRIVE") + Environment.GetEnvironmentVariable("HOMEPATH");
                if (!string.IsNullOrEmpty(homeFolder) && File.Exists(Path.Combine(homeFolder, ".mynosqlserver")))
                {
                    FileStream fileStream = new FileStream(Path.Combine(homeFolder, ".mynosqlserver"), FileMode.Open);
                    configBuilder.AddJsonStream(fileStream);
                }

                configBuilder.AddEnvironmentVariables();

                var config = configBuilder.Build();

                var data = config.Get<SettingsModel>();

                if (string.IsNullOrEmpty(data.BackupAzureConnectString))
                {
                    Console.WriteLine("No connection string found. Backups are disabled");
                    Console.WriteLine("In case to enable, please specify 'BackupAzureConnectString' in env variable or in json file: ~/.mynosqlserver");
                }

                return data;
            }
            catch (Exception e)
            {
                Console.WriteLine("Error reading Settings File: "+e.Message);
                Console.WriteLine("Backups are disabled");
                return new SettingsModel();
            }
        }
    }
    
}