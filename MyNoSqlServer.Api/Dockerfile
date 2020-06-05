FROM mcr.microsoft.com/dotnet/core/aspnet:3.1-buster-slim
WORKDIR /app
EXPOSE 5001
EXPOSE 5000
COPY . .
ENTRYPOINT ["dotnet", "MyNoSqlServer.Api.dll"]
