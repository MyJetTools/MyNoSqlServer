using System;
using System.IO;
using MyNoSqlServer.Domains;
using MyNoSqlServer.NodePersistence;
using MyYamlParser;

namespace MyNoSqlServer.Api
{
    public class SettingsModel : IMyNoSqlNodePersistenceSettings, ISettingsLocation
    {
        [YamlProperty]
        public string PersistenceDest { get; set; }
        
        [YamlProperty]
        public bool CompressData { get; set; }
        [YamlProperty]
        public int MaxPayloadSize { get; set; }
        [YamlProperty]
        public string Location { get; set; }


        public bool IsNode()
        {
            return PersistenceDest.StartsWith("http");
        }
    }

    public static class SettingsLoader
    {
        public static SettingsModel LoadSettings()
        {
            try
            {


                byte[] settingsContent = null;
                
                
                const string fileName = ".mynosqlserver";
                

                var homeFolder = Environment.GetEnvironmentVariable("HOME");
                if (!string.IsNullOrEmpty(homeFolder) && File.Exists(Path.Combine(homeFolder, fileName)))
                {
                    var path = Path.Combine(homeFolder, fileName);
                    settingsContent = File.ReadAllBytes(path);
                }
                else
                {
                    Console.WriteLine("Settings File not found as Linux or MacOs Path. Skipping");
                }

                if (settingsContent == null)
                {
                    homeFolder = Environment.GetEnvironmentVariable("HOMEDRIVE") + Environment.GetEnvironmentVariable("HOMEPATH");
                    if (!string.IsNullOrEmpty(homeFolder) && File.Exists(Path.Combine(homeFolder, fileName)))
                    {
                        settingsContent = File.ReadAllBytes(Path.Combine(homeFolder, fileName));
                    }
                    else
                    {
                        Console.WriteLine("Settings File not found as Windows Path. Skipping");
                    }
                }
    

                if (settingsContent == null)
                {
                    throw new Exception("Settings file not found at HOME/" + fileName + " path");
                }
                

                var settingsModel = MyYamlDeserializer.Deserialize<SettingsModel>(settingsContent);

  

                if (string.IsNullOrEmpty(settingsModel.PersistenceDest))
                {
                    Console.WriteLine("No connection string found. Backups are disabled");
                    Console.WriteLine("In case to enable, please specify 'BackupAzureConnectString' in env variable or in json file: ~/.mynosqlserver");
                    throw new Exception("PersistencePath should not be Empty");
                }

                if (!settingsModel.PersistenceDest.StartsWith("http") && !settingsModel.PersistenceDest.StartsWith("DefaultEndpointsProtocol"))
                {
                    throw new Exception(
                        "PersistencePath Parameter should start either from http/https for GRPC Dependency or from DefaultEndpointsProtocol");
                }

                if (settingsModel.MaxPayloadSize < 1024 * 1024)
                {
                    throw new Exception($"MaxPayloadSize settings must be greater then {1024 * 1024}");
                }

                return settingsModel;
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