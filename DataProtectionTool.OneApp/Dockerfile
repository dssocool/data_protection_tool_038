FROM node:22-alpine AS frontend-build
WORKDIR /app/frontend
COPY DataProtectionTool.OneApp/frontend/package.json DataProtectionTool.OneApp/frontend/package-lock.json* ./
RUN npm ci
COPY DataProtectionTool.OneApp/frontend/ ./
RUN npm run build -- --outDir /app/wwwroot

FROM mcr.microsoft.com/dotnet/sdk:9.0 AS dotnet-build
WORKDIR /src
COPY DataProtectionTool.OneApp/DataProtectionTool.OneApp.csproj DataProtectionTool.OneApp/
RUN dotnet restore DataProtectionTool.OneApp/DataProtectionTool.OneApp.csproj
COPY DataProtectionTool.OneApp/ DataProtectionTool.OneApp/
COPY --from=frontend-build /app/wwwroot DataProtectionTool.OneApp/wwwroot/
RUN dotnet publish DataProtectionTool.OneApp/DataProtectionTool.OneApp.csproj \
    -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS runtime
WORKDIR /app
COPY --from=dotnet-build /app/publish .
EXPOSE 8190
ENTRYPOINT ["dotnet", "DataProtectionTool.OneApp.dll"]
