# ---- build ----
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY orchestrator-dotnet/*.csproj orchestrator-dotnet/
RUN dotnet restore orchestrator-dotnet/*.csproj

COPY . .
RUN dotnet publish orchestrator-dotnet/*.csproj -c Release -o /app

# ---- run ----
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS final
WORKDIR /app
COPY --from=build /app .

ENV ASPNETCORE_URLS=http://0.0.0.0:${PORT}
# DLL adını log’da gördüğümüz ile eşliyoruz:
ENTRYPOINT ["dotnet", "Orchestrator.Api.dll"]
