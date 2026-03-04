FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
WORKDIR /app
EXPOSE 8080

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY ["IT4You.API/IT4You.API.csproj", "IT4You.API/"]
COPY ["IT4You.Application/IT4You.Application.csproj", "IT4You.Application/"]
COPY ["IT4You.Domain/IT4You.Domain.csproj", "IT4You.Domain/"]
COPY ["IT4You.Infrastructure/IT4You.Infrastructure.csproj", "IT4You.Infrastructure/"]
RUN dotnet restore "IT4You.API/IT4You.API.csproj"
COPY . .
WORKDIR "/src/IT4You.API"
RUN dotnet build "IT4You.API.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "IT4You.API.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "IT4You.API.dll"]
