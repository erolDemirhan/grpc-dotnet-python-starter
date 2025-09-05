# build
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY orchestrator-dotnet/Orchestrator.Api.csproj orchestrator-dotnet/
COPY protos/ protos/
RUN dotnet restore orchestrator-dotnet/Orchestrator.Api.csproj
COPY . .
RUN dotnet publish orchestrator-dotnet/Orchestrator.Api.csproj -c Release -o /app/publish

# runtime
FROM mcr.microsoft.com/dotnet/aspnet:8.0
WORKDIR /app
COPY --from=build /app/publish ./
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
ENTRYPOINT ["dotnet","Orchestrator.Api.dll"]
