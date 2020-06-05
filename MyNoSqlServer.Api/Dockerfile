FROM mcr.microsoft.com/dotnet/core/aspnet:3.1.1
WORKDIR /app
COPY /app /app

ARG app_version
ARG app_compilation_date
ENV APP_VERSION=${app_version}
ENV APP_COMPILATION_DATE=${app_compilation_date}

ENTRYPOINT ["dotnet", "MyNoSqlServer.Api.dll"]