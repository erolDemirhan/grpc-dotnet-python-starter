# ---- build ----
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# Proje tanımı: csproj adını klasöre kopyalayarak restore yap
COPY orchestrator-dotnet/*.csproj orchestrator-dotnet/
RUN dotnet restore orchestrator-dotnet/*.csproj

# Tüm kaynakları kopyala ve publish et
COPY . .
RUN dotnet publish orchestrator-dotnet/*.csproj -c Release -o /app

# ---- run ----
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS final
WORKDIR /app
COPY --from=build /app .

# Render portu: 0.0.0.0 ve ${PORT} dinle
ENV ASPNETCORE_URLS=http://0.0.0.0:${PORT}

# Proje DLL adını klasörle aynı varsaydım; farklıysa değiştir:
ENTRYPOINT ["dotnet", "orchestrator-dotnet.dll"]
