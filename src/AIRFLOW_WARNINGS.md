# Airflow Webserver Warnings - Resolution Guide

This document explains the warnings seen in Airflow webserver logs and how to address them.

## ✅ Fixed: Flask-Limiter In-Memory Storage Warning

**Warning:**
```
UserWarning: Using the in-memory storage for tracking rate limits as no storage was explicitly specified. This is not recommended for production use.
```

**Status:** ✅ **FIXED**

**Solution:** Configured Flask-Limiter to use Redis as the storage backend in `docker-compose.yml`:
```yaml
RATELIMIT_STORAGE_URL: "redis://:@redis:6379/1"
```

This uses Redis database 1 (separate from the Celery broker which uses database 0) to store rate limit tracking data. This ensures:
- Rate limits persist across webserver restarts
- Rate limits are shared across multiple webserver instances
- Production-ready configuration

**Action Required:** Restart the `airflow-webserver` service for the change to take effect:
```bash
docker-compose restart airflow-webserver
```

---

## ⚠️ Third-Party Library Warnings (Cannot Fix Directly)

### 1. Marshmallow Deprecation Warnings

**Warning:**
```
ChangedInMarshmallow4Warning: `Number` field should not be instantiated. Use `Integer`, `Float`, or `Decimal` instead.
```

**Status:** ⚠️ **Cannot Fix Directly**

**Explanation:** These warnings come from Airflow's internal code (`airflow/api_connexion/schemas/task_schema.py`). The Airflow development team needs to update their code to use the new Marshmallow 4 API.

**Impact:** Low - These are deprecation warnings, not errors. Functionality is not affected.

**Action:** 
- Monitor Airflow releases for updates
- Consider upgrading to newer Airflow versions when available
- These warnings will be resolved in future Airflow releases

---

### 2. Azure Batch SDK Syntax Warning

**Warning:**
```
SyntaxWarning: invalid escape sequence '\s' in azure/batch/models/_models_py3.py:4839
```

**Status:** ⚠️ **Cannot Fix Directly**

**Explanation:** This warning comes from the Azure Batch SDK library itself. It's a minor syntax issue in the Azure SDK code.

**Impact:** Very Low - This is a syntax warning, not an error. The code still functions correctly.

**Action:**
- Monitor Azure SDK releases for fixes
- Update `azure-batch` package when a fix is available
- This warning can be safely ignored

---

### 3. Optional Provider Feature Disabled

**Message:**
```
INFO - Optional provider feature disabled when importing 'airflow.providers.google.leveldb.hooks.leveldb.LevelDBHook'
```

**Status:** ℹ️ **Informational Only**

**Explanation:** This is an informational message, not a warning. Airflow is indicating that an optional Google LevelDB provider feature is disabled because the required dependencies are not installed.

**Impact:** None - This is expected behavior if you're not using Google LevelDB.

**Action:** No action needed unless you specifically need Google LevelDB functionality.

---

## Summary

| Warning | Status | Action Required |
|---------|--------|-----------------|
| Flask-Limiter in-memory storage | ✅ Fixed | Restart webserver |
| Marshmallow deprecation | ⚠️ Third-party | Monitor Airflow updates |
| Azure Batch syntax warning | ⚠️ Third-party | Monitor Azure SDK updates |
| Optional provider disabled | ℹ️ Informational | None |

---

## Verification

After restarting the webserver, check the logs to confirm the Flask-Limiter warning is gone:

```bash
docker-compose logs airflow-webserver | grep -i "flask-limiter\|ratelimit"
```

You should no longer see the in-memory storage warning.

