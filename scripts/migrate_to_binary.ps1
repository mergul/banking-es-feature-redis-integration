# Redis Binary Migration Script for Windows
# This script helps migrate from JSON to binary serialization

param(
    [string]$RedisHost = "localhost",
    [int]$RedisPort = 6379,
    [string]$RedisPassword = "",
    [switch]$ClearAll,
    [switch]$BuildOnly,
    [switch]$ClearCacheOnly
)

# Set error action preference
$ErrorActionPreference = "Stop"

Write-Host "🚀 Starting Redis Binary Migration..." -ForegroundColor Blue

# Configuration
$env:REDIS_HOST = $RedisHost
$env:REDIS_PORT = $RedisPort
$env:REDIS_PASSWORD = $RedisPassword

Write-Host "Configuration:" -ForegroundColor Blue
Write-Host "  Redis Host: $RedisHost"
Write-Host "  Redis Port: $RedisPort"
Write-Host "  Redis Password: $(if($RedisPassword) { "***" } else { "None" })"
Write-Host ""

# Function to check if Redis is accessible
function Test-RedisConnection {
    Write-Host "Checking Redis connectivity..." -ForegroundColor Blue
    
    try {
        if ($RedisPassword) {
            redis-cli -h $RedisHost -p $RedisPort -a $RedisPassword ping | Out-Null
        } else {
            redis-cli -h $RedisHost -p $RedisPort ping | Out-Null
        }
        Write-Host "✅ Redis is accessible" -ForegroundColor Green
        return $true
    }
    catch {
        Write-Host "❌ Cannot connect to Redis" -ForegroundColor Red
        Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
        return $false
    }
}

# Function to backup Redis data
function Backup-Redis {
    Write-Host "Creating Redis backup..." -ForegroundColor Blue
    
    $backupFile = "redis_backup_$(Get-Date -Format 'yyyyMMdd_HHmmss').rdb"
    
    try {
        if ($RedisPassword) {
            redis-cli -h $RedisHost -p $RedisPort -a $RedisPassword BGSAVE
        } else {
            redis-cli -h $RedisHost -p $RedisPort BGSAVE
        }
        Write-Host "✅ Redis backup initiated: $backupFile" -ForegroundColor Green
        Write-Host "⚠️  Note: Backup file location depends on Redis configuration" -ForegroundColor Yellow
    }
    catch {
        Write-Host "⚠️  Warning: Could not create backup - $($_.Exception.Message)" -ForegroundColor Yellow
    }
}

# Function to clear Redis cache
function Clear-RedisCache {
    param([switch]$All)
    
    Write-Host "Clearing Redis cache..." -ForegroundColor Blue
    
    if ($All) {
        Write-Host "⚠️  Clearing ALL Redis data..." -ForegroundColor Yellow
        $confirm = Read-Host "Are you sure you want to clear ALL Redis data? (y/N)"
        if ($confirm -eq 'y' -or $confirm -eq 'Y') {
            try {
                if ($RedisPassword) {
                    redis-cli -h $RedisHost -p $RedisPort -a $RedisPassword FLUSHALL
                } else {
                    redis-cli -h $RedisHost -p $RedisPort FLUSHALL
                }
                Write-Host "✅ All Redis data cleared" -ForegroundColor Green
            }
            catch {
                Write-Host "❌ Failed to clear Redis data: $($_.Exception.Message)" -ForegroundColor Red
                throw
            }
        } else {
            Write-Host "⚠️  Skipping cache clear" -ForegroundColor Yellow
        }
    } else {
        Write-Host "Clearing only banking-related keys..." -ForegroundColor Blue
        
        try {
            # Clear account keys
            $accountKeysScript = @"
local keys = redis.call('keys', 'account:*')
if #keys > 0 then
    return redis.call('del', unpack(keys))
else
    return 0
end
"@
            
            $eventsKeysScript = @"
local keys = redis.call('keys', 'events:*')
if #keys > 0 then
    return redis.call('del', unpack(keys))
else
    return 0
end
"@
            
            if ($RedisPassword) {
                redis-cli -h $RedisHost -p $RedisPort -a $RedisPassword --eval - $accountKeysScript
                redis-cli -h $RedisHost -p $RedisPort -a $RedisPassword --eval - $eventsKeysScript
            } else {
                redis-cli -h $RedisHost -p $RedisPort --eval - $accountKeysScript
                redis-cli -h $RedisHost -p $RedisPort --eval - $eventsKeysScript
            }
            
            Write-Host "✅ Banking-related cache cleared" -ForegroundColor Green
        }
        catch {
            Write-Host "❌ Failed to clear cache: $($_.Exception.Message)" -ForegroundColor Red
            throw
        }
    }
}

# Function to build and test the application
function Build-And-Test {
    Write-Host "Building application..." -ForegroundColor Blue
    
    try {
        # Build the application
        cargo build --release
        if ($LASTEXITCODE -eq 0) {
            Write-Host "✅ Build successful" -ForegroundColor Green
        } else {
            Write-Host "❌ Build failed" -ForegroundColor Red
            throw "Build failed with exit code $LASTEXITCODE"
        }
        
        Write-Host "Running tests..." -ForegroundColor Blue
        
        # Run tests
        cargo test
        if ($LASTEXITCODE -eq 0) {
            Write-Host "✅ Tests passed" -ForegroundColor Green
        } else {
            Write-Host "❌ Tests failed" -ForegroundColor Red
            throw "Tests failed with exit code $LASTEXITCODE"
        }
    }
    catch {
        Write-Host "❌ Build or test failed: $($_.Exception.Message)" -ForegroundColor Red
        throw
    }
}

# Function to restart services
function Restart-Services {
    Write-Host "Restarting services..." -ForegroundColor Blue
    
    Write-Host "⚠️  Please restart your application services manually:" -ForegroundColor Yellow
    Write-Host "   - Stop current application instances" -ForegroundColor Yellow
    Write-Host "   - Deploy new binary" -ForegroundColor Yellow
    Write-Host "   - Start application instances" -ForegroundColor Yellow
}

# Function to verify migration
function Verify-Migration {
    Write-Host "Verifying migration..." -ForegroundColor Blue
    
    Write-Host "⚠️  Please verify manually:" -ForegroundColor Yellow
    Write-Host "   1. Check Redis for new binary data" -ForegroundColor Yellow
    Write-Host "   2. Verify cache operations work correctly" -ForegroundColor Yellow
    Write-Host "   3. Test Kafka message production/consumption" -ForegroundColor Yellow
    Write-Host "   4. Monitor application logs for errors" -ForegroundColor Yellow
}

# Main execution
function Main {
    Write-Host "=== Redis Binary Migration Script ===" -ForegroundColor Blue
    Write-Host ""
    
    # Check Redis connectivity
    if (-not (Test-RedisConnection)) {
        exit 1
    }
    
    # Determine action based on parameters
    if ($BuildOnly) {
        Write-Host "Building and testing only..." -ForegroundColor Blue
        Build-And-Test
    }
    elseif ($ClearCacheOnly) {
        Write-Host "Clearing cache only..." -ForegroundColor Blue
        Clear-RedisCache
    }
    else {
        # Interactive mode or full migration
        if ($ClearAll) {
            Write-Host "Starting full migration..." -ForegroundColor Blue
            Backup-Redis
            Clear-RedisCache -All
            Build-And-Test
            Restart-Services
            Verify-Migration
        }
        else {
            Write-Host "Starting safe migration..." -ForegroundColor Blue
            Backup-Redis
            Clear-RedisCache
            Build-And-Test
            Restart-Services
            Verify-Migration
        }
    }
    
    Write-Host ""
    Write-Host "🎉 Migration completed!" -ForegroundColor Green
    Write-Host "Remember to monitor your application for any issues." -ForegroundColor Blue
}

# Run main function
try {
    Main
}
catch {
    Write-Host "❌ Migration failed: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
} 