# Redis Binary Migration - Deployment Guide

## 🚀 Quick Start

### Option 1: Using PowerShell Script (Recommended for Windows)

```powershell
# Run the migration script
.\scripts\migrate_to_binary.ps1

# Or with specific options:
.\scripts\migrate_to_binary.ps1 -BuildOnly
.\scripts\migrate_to_binary.ps1 -ClearCacheOnly
.\scripts\migrate_to_binary.ps1 -ClearAll
```

### Option 2: Manual Steps

#### 1. Build and Test
```bash
# Build the application
cargo build --release

# Run tests
cargo test
```

#### 2. Clear Redis Cache
```bash
# Connect to Redis
redis-cli

# Clear all data (if you want to start fresh)
FLUSHALL

# Or clear only banking-related keys
DEL account:*
DEL events:*
```

#### 3. Deploy New Binary
```bash
# Stop your current application
# Deploy the new binary
# Start the application
```

## 📋 Pre-Deployment Checklist

- [ ] ✅ Code changes are complete (JSON → Binary serialization)
- [ ] ✅ All tests pass (`cargo test`)
- [ ] ✅ Application builds successfully (`cargo build --release`)
- [ ] ✅ Redis is accessible and configured
- [ ] ✅ Backup strategy is in place
- [ ] ✅ Rollback plan is ready
- [ ] ✅ Monitoring is set up

## 🔧 Configuration

### Redis Configuration
Make sure your Redis configuration supports binary data:

```conf
# redis.conf
maxmemory 4gb
maxmemory-policy allkeys-lru
```

### Environment Variables
```bash
# Redis connection
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=your_password_here
```

## 📊 Monitoring

After deployment, monitor these metrics:

### Performance Metrics
- Cache hit/miss rates
- Serialization/deserialization times
- Memory usage in Redis
- Network bandwidth usage

### Error Metrics
- Serialization errors
- Deserialization errors
- Cache operation failures
- Kafka message processing errors

### Application Metrics
- Response times
- Throughput
- Error rates
- Resource utilization

## 🚨 Troubleshooting

### Common Issues

#### 1. Serialization Errors
```
Error: Failed to deserialize account from cache
```
**Solution**: Clear Redis cache and restart application

#### 2. Cache Misses
```
High cache miss rate after migration
```
**Solution**: This is expected - cache will rebuild with binary data

#### 3. Memory Usage
```
Redis memory usage increased
```
**Solution**: Monitor and adjust `maxmemory` settings if needed

### Rollback Procedure

If issues occur:

1. **Stop the application**
2. **Revert code changes** (JSON serialization)
3. **Clear Redis cache**
4. **Redeploy with old code**
5. **Restart application**

## 📈 Expected Benefits

After successful migration:

- **50-100% faster** serialization
- **100-200% faster** deserialization  
- **20-40% smaller** payload sizes
- **10-30% less** memory usage
- **Consistent** data format across all components

## 🔍 Verification

### 1. Check Redis Data Format
```bash
# Connect to Redis
redis-cli

# Check a sample key
GET account:some-uuid
# Should return binary data (not readable JSON)
```

### 2. Verify Cache Operations
```bash
# Test cache operations through your application
# Monitor logs for any serialization errors
```

### 3. Test Kafka Messages
```bash
# Verify Kafka messages are in binary format
# Check consumer logs for deserialization success
```

## 📞 Support

If you encounter issues:

1. Check the application logs
2. Verify Redis connectivity
3. Test with a simple cache operation
4. Review the migration documentation (`REDIS_BINARY_MIGRATION.md`)

## 🎯 Success Criteria

Migration is successful when:

- ✅ Application starts without errors
- ✅ Cache operations work correctly
- ✅ Kafka messages are processed successfully
- ✅ Performance metrics show improvement
- ✅ No serialization/deserialization errors in logs
- ✅ Memory usage is within expected ranges 