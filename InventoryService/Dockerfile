# ===== BUILD STAGE =====
# Use the full .NET SDK because we need to restore, compile and publish the application.
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build

WORKDIR /src

# Copy only the project file first.
# This allows Docker to cache the NuGet restore layer when source code changes.
COPY ["InventoryService.csproj", "./"]

RUN dotnet restore "InventoryService.csproj"

# Copy the rest of the InventoryService source code.
COPY . .

# Build and publish the application in Release mode.
RUN dotnet publish "InventoryService.csproj" \
    -c Release \
    -o /app/publish


# ===== RUNTIME STAGE =====
# Use the smaller ASP.NET runtime image because compilation is already finished.
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final

WORKDIR /app

# Copy only the published application from the build stage.
COPY --from=build /app/publish .

# Start InventoryService when the container starts.
ENTRYPOINT ["dotnet", "InventoryService.dll"]